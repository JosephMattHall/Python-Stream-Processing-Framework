import argparse
import sys
import asyncio
from pspf.utils.logging import setup_logging

def main() -> None:
    parser = argparse.ArgumentParser(description="PSPF Command Line Interface")
    parser.add_argument("mode", choices=["run", "inspect"], help="PSPF execution mode")
    parser.add_argument("--job", type=str, help="Job name / checkpoint ID")
    parser.add_argument("--log-level", type=str, default="INFO", help="Logging level")
    parser.add_argument("--checkpoint-interval", type=float, default=0.0, help="Checkpoint interval in seconds")

    args = parser.parse_args()
    setup_logging(level=args.log_level)
    
    if args.mode == "run":
        print(f"PSPF: Starting job '{args.job}' (Log Level: {args.log_level})")
        # In a real system, we'd load the pipeline from a file/module here
    elif args.mode == "inspect":
        print(f"PSPF: Inspecting state for job '{args.job}'")

if __name__ == "__main__":
    main()
