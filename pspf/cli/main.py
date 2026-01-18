import argparse
import asyncio
import json
from pspf.runtime.valkey_store import ValkeyOffsetStore
from pspf.operators.state import ValkeyKeyedState

def main():
    parser = argparse.ArgumentParser(description="PSPF Operational CLI")
    subparsers = parser.add_subparsers(dest="command")

    # Offset Management
    offset_parser = subparsers.add_parser("offsets", help="Manage consumer offsets")
    offset_parser.add_argument("action", choices=["get", "set"])
    offset_parser.add_argument("--group", required=True)
    offset_parser.add_argument("--partition", type=int, required=True)
    offset_parser.add_argument("--offset", type=int)

    # State Inspection
    state_parser = subparsers.add_parser("state", help="Inspect operator state")
    state_parser.add_argument("action", choices=["get", "clear"])
    state_parser.add_argument("--prefix", required=True, help="State prefix (e.g. 'inventory')")
    state_parser.add_argument("--key", required=True)

    args = parser.parse_args()

    if args.command == "offsets":
        asyncio.run(handle_offsets(args))
    elif args.command == "state":
        asyncio.run(handle_state(args))
    else:
        parser.print_help()

async def handle_offsets(args):
    store = ValkeyOffsetStore()
    if args.action == "get":
        offset = await store.get(args.group, args.partition)
        print(f"Group: {args.group}, Partition: {args.partition}, Offset: {offset}")
    elif args.action == "set":
        if args.offset is None:
            print("Error: --offset required for 'set' action")
            return
        await store.commit(args.group, args.partition, args.offset)
        print(f"Successfully set Group: {args.group}, Partition: {args.partition} to Offset: {args.offset}")

async def handle_state(args):
    # Note: We need msgpack to unpack the state
    state = ValkeyKeyedState(prefix=args.prefix)
    if args.action == "get":
        val = await state.get(args.key)
        print(f"Key: {args.key}, State: {json.dumps(val, indent=2) if val else 'None'}")
    elif args.action == "clear":
        await state.clear(args.key)
        print(f"Cleared state for Key: {args.key}")

if __name__ == "__main__":
    main()
