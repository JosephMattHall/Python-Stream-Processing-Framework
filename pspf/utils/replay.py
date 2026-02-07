import asyncio
import logging
import argparse
import sys
from pspf.connectors.valkey import ValkeyConnector
from pspf.settings import settings
from pspf.utils.logging import setup_logging

setup_logging()
logger = logging.getLogger("pspf.replay")

async def replay_dead_letters(source_dlq: str, target_stream: str, count: int = 100, delete: bool = False):
    """
    Reads messages from DLQ and re-injects them into the target stream.
    """
    connector = ValkeyConnector(host=settings.VALKEY_HOST, port=settings.VALKEY_PORT)
    await connector.connect()
    client = connector.get_client()

    try:
        # Read from DLQ (using XREAD, not group, simpler for tooling)
        # We start from beginning '0' or last ID? For simple tool: 0
        streams = await client.xread({source_dlq: "0"}, count=count, block=1000)
        
        if not streams:
            logger.info("No messages found in DLQ.")
            return

        _, messages = streams[0]
        logger.info(f"Found {len(messages)} messages in {source_dlq}")

        replayed_count = 0
        for msg_id, data in messages:
            # Strip metadata
            clean_data = {k: v for k, v in data.items() if not k.startswith("_")}
            
            # Re-inject
            new_id = await client.xadd(target_stream, clean_data)
            logger.info(f"Replayed {msg_id} -> {new_id} in {target_stream}")
            replayed_count += 1
            
            # Delete if requested
            if delete:
                await client.xdel(source_dlq, msg_id)

        logger.info(f"Successfully replayed {replayed_count} messages.")

    except Exception as e:
        logger.error(f"Replay failed: {e}")
    finally:
        await connector.close()

async def main():
    parser = argparse.ArgumentParser(description="Replay Dead Letter Queue messages.")
    parser.add_argument("dlq_stream", help="Name of the DLQ stream (e.g. events-dlq)")
    parser.add_argument("target_stream", help="Name of the target stream (e.g. events)")
    parser.add_argument("--count", type=int, default=100, help="Max messages to replay")
    parser.add_argument("--delete", action="store_true", help="Delete from DLQ after replay")

    args = parser.parse_args()

    await replay_dead_letters(args.dlq_stream, args.target_stream, args.count, args.delete)

if __name__ == "__main__":
    asyncio.run(main())
