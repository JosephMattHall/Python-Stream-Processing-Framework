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

app = typer.Typer(help="PSPF Control Interface")

@app.command()
def inspect(
    data_dir: Path = typer.Argument(..., help="Path to the LocalLog data directory"),
    partition: int = typer.Option(0, help="Partition to inspect"),
    limit: int = typer.Option(10, help="Number of records to show"),
    tail: bool = typer.Option(False, help="Show last N records instead of first N")
):
    """
    Inspects the local log files directly.
    """
    if not data_dir.exists():
        typer.echo(f"Error: Directory {data_dir} does not exist.")
        raise typer.Exit(code=1)

    async def _inspect():
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
def status(url: str = typer.Option("http://localhost:8001", help="URL of the Admin API")):
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
def pause(url: str = typer.Option("http://localhost:8001", help="URL of the Admin API")):
    """
    Pauses the worker's message consumption.
    """
    _send_control(url, "pause")

@app.command()
def resume(url: str = typer.Option("http://localhost:8001", help="URL of the Admin API")):
    """
    Resumes the worker's message consumption.
    """
    _send_control(url, "resume")

def _send_control(base_url: str, action: str):
    url = f"{base_url}/control/{action}"
    try:
        r = httpx.post(url)
        if r.status_code == 200:
            typer.echo(f"✅ Command '{action}' sent successfully.")
        else:
            typer.echo(f"⚠️  Failed to {action}: {r.status_code} {r.text}")
    except httpx.RequestError as e:
        typer.echo(f"❌ Connection error: {e}")

if __name__ == "__main__":
    app()
