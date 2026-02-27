import typer
import asyncio
import httpx
import sys
import json
from pathlib import Path
from typing import Optional
from datetime import datetime

# Import LocalLog for inspection
from pspf.log.local_log import LocalLog
from pspf.utils.logging import setup_logging
from pspf.connectors.valkey import ValkeyConnector, ValkeyStreamBackend
from pspf.settings import settings

app = typer.Typer(help="PSPF Control Interface")

@app.command()
def inspect(
    data_dir: Path = typer.Argument(..., help="Path to the LocalLog data directory"),
    partition: int = typer.Option(0, help="Partition to inspect"),
    limit: int = typer.Option(10, help="Number of records to show"),
    tail: bool = typer.Option(False, help="Show last N records instead of first N")
) -> None:
    """
    Inspects the local log files directly.
    """
    if not data_dir.exists():
        typer.echo(f"Error: Directory {data_dir} does not exist.")
        raise typer.Exit(code=1)

    async def _inspect() -> None:
        log = LocalLog(str(data_dir), num_partitions=1) # Num partitions doesn't matter for reading specific p
        
        # Get High Watermark
        hw = await log.get_high_watermark(partition)
        typer.echo(f"Partition {partition}: High Watermark = {hw}")
        
        start_offset = 0
        if tail:
            start_offset = max(0, hw - limit)
            
        count = 0
        typer.echo(f"--- Records (Offset {start_offset} to {start_offset+limit}) ---")
        
        async for record in log.read(partition, start_offset):
            if count >= limit:
                break
            
            # Formatting
            ts = record.timestamp.isoformat()
            typer.echo(f"[{record.offset}] {ts} | {record.key} | {json.dumps(record.value)}")
            count += 1
            
    asyncio.run(_inspect())

@app.command()
def status(url: str = typer.Option("http://localhost:8001", help="URL of the Admin API")) -> None:
    """
    Checks the status of a PSPF worker.
    """
    try:
        r = httpx.get(f"{url}/health")
        if r.status_code == 200:
            data = r.json()
            typer.echo(f"✅ Worker Online: {data}")
        else:
            typer.echo(f"⚠️  Worker returned {r.status_code}: {r.text}")
    except httpx.RequestError as e:
        typer.echo(f"❌ Failed to connect to {url}: {e}")

@app.command()
def pause(url: str = typer.Option("http://localhost:8001", help="URL of the Admin API")) -> None:
    """
    Pauses the worker's message consumption.
    """
    _send_control(url, "pause")

@app.command()
def resume(url: str = typer.Option("http://localhost:8001", help="URL of the Admin API")) -> None:
    """
    Resumes the worker's message consumption.
    """
    _send_control(url, "resume")

def _send_control(base_url: str, action: str) -> None:
    url = f"{base_url}/control/{action}"
    try:
        r = httpx.post(url)
        if r.status_code == 200:
            typer.echo(f"✅ Command '{action}' sent successfully.")
        else:
            typer.echo(f"⚠️  Failed to {action}: {r.status_code} {r.text}")
    except httpx.RequestError as e:
        typer.echo(f"❌ Connection error: {e}")

@app.command("cluster-status")
def cluster_status(url: str = typer.Option("http://localhost:8001", help="URL of the Admin API")) -> None:
    """
    Shows detailed cluster topology and partition assignments.
    """
    try:
        r = httpx.get(f"{url}/cluster/status")
        if r.status_code == 200:
            data = r.json()
            if not data.get("ha_enabled"):
                typer.echo("ℹ️  HA/Clustering is not enabled on this worker.")
                return
                
            typer.echo(f"🔵 Node ID: {data.get('node_id')}")
            typer.echo(f"   Held Partitions: {data.get('held_partitions', [])}")
            
            nodes = data.get("nodes", [])
            typer.echo(f"   Other Cluster Nodes ({len(nodes)}):")
            for n in nodes:
                typer.echo(f"     - {n.get('id')} at {n.get('host')}:{n.get('port')}")
        else:
            typer.echo(f"⚠️  Worker returned {r.status_code}: {r.text}")
    except httpx.RequestError as e:
        typer.echo(f"❌ Failed to connect to {url}: {e}")

@app.command()
def groups(stream: str = typer.Argument(..., help="Stream name to query")) -> None:
    """Lists consumer groups for a given stream."""
    async def _groups() -> None:
        connector = ValkeyConnector(host=settings.valkey.HOST, port=settings.valkey.PORT)
        async with connector:
            client = connector.get_client()
            try:
                groups = await client.xinfo_groups(stream)
                if not groups:
                    typer.echo(f"No consumer groups found for stream '{stream}'.")
                    return
                
                typer.echo(f"--- Consumer Groups for '{stream}' ---")
                for g in groups:
                    typer.echo(f"Name: {g['name']} | Consumers: {g['consumers']} | Pending: {g['pending']} | Lag: {g.get('lag', 'N/A')}")
            except Exception as e:
                typer.echo(f"Error: {e}")
    
    asyncio.run(_groups())

@app.command()
def reset(
    stream: str = typer.Argument(..., help="Stream name"),
    group: str = typer.Argument(..., help="Consumer group name"),
    offset: str = typer.Argument("0", help="Offset to reset to (0, $, or specific ID)")
) -> None:
    """Resets the offset for a consumer group."""
    async def _reset() -> None:
        connector = ValkeyConnector(host=settings.valkey.HOST, port=settings.valkey.PORT)
        async with connector:
            client = connector.get_client()
            try:
                await client.xgroup_setid(stream, group, offset)
                typer.echo(f"✅ Reset group '{group}' on stream '{stream}' to offset '{offset}'.")
            except Exception as e:
                typer.echo(f"❌ Failed to reset offset: {e}")
                
    asyncio.run(_reset())

@app.command()
def replay(
    stream: str = typer.Argument(..., help="Source stream (e.g. 'orders')"),
    group: str = typer.Argument(..., help="Consumer group that failed"),
    limit: int = typer.Option(100, help="Max messages to replay")
) -> None:
    """Replays messages from DLQ back to the main stream."""
    dlq_stream = f"{stream}-dlq"
    
    async def _replay() -> None:
        connector = ValkeyConnector(host=settings.valkey.HOST, port=settings.valkey.PORT)
        async with connector:
            client = connector.get_client()
            try:
                # 1. Read from DLQ (using 0 to read all)
                # Note: We read as 0-0 since it's not a group read usually for DLQ inspection
                messages = await client.xrange(dlq_stream, min="-", max="+", count=limit)
                if not messages:
                    typer.echo(f"No messages found in DLQ '{dlq_stream}'.")
                    return
                
                typer.echo(f"Found {len(messages)} messages in {dlq_stream}. Replaying...")
                
                replayed_count = 0
                for msg_id, data in messages:
                    # Clean up DLQ metadata before re-injecting? 
                    # Usually better to keep it so we know it was replayed.
                    # Remove internal keys if they start with _ and were added by DLQ logic
                    new_data = {k: v for k, v in data.items() if not k.startswith("_")}
                    
                    # Add to main stream
                    await client.xadd(stream, new_data)
                    
                    # Remove from DLQ
                    await client.xdel(dlq_stream, msg_id)
                    replayed_count += 1
                
                typer.echo(f"✅ Successfully replayed {replayed_count} messages to '{stream}'.")
            except Exception as e:
                typer.echo(f"❌ Replay failed: {e}")

    asyncio.run(_replay())

if __name__ == "__main__":
    app()
