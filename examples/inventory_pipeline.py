import os
import asyncio
from pspf.operators.core import Pipeline
from pspf.connectors.file import FileSource, StorageSink, ConsoleSink

def main():
    # Create a dummy input file
    input_file = "inventory_data.txt"
    with open(input_file, "w") as f:
        f.write("item:apple,qty:10\n")
        f.write("item:banana,qty:5\n")
        f.write("item:apple,qty:2\n")
        f.write("item:orange,qty:8\n")

    print("--- Starting Inventory Pipeline ---")

    # Build the pipeline
    p = Pipeline()
    (p.read_from(FileSource(input_file, delay=0.1))
      .map(lambda line: line.split(",")) # ["item:apple", "qty:10"]
      .map(lambda parts: (parts[0].split(":")[1], int(parts[1].split(":")[1]))) # ("apple", 10)
      # Reduce sum per key (item)
      .key_by(lambda x: x[0]) 
      .reduce(lambda acc, current: (acc[0], acc[1] + current[1])) 
      .write_to(ConsoleSink(prefix="[INVENTORY] ")))

    # Run it
    p.run()
    
    print("--- Pipeline Finished ---")
    
    # Cleanup
    if os.path.exists(input_file):
        os.remove(input_file)

if __name__ == "__main__":
    main()
