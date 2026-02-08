import sys
import os

# Ensure project root is in path
sys.path.insert(0, os.getcwd())

from examples.verify_refactor import main
import asyncio

if __name__ == "__main__":
    asyncio.run(main())
