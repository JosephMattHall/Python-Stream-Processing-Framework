# Python Stream Processing Framework (PSPF)

**A Python‑native, async stream processing framework for building stateful, fault‑tolerant, event‑driven systems.**

PSPF provides a lightweight, extensible runtime for consuming event streams, transforming data with operator pipelines, and managing state with checkpoint‑based recovery — all written primarily in Python.

---

## ✨ Features

- 🧩 **Composable Operators** — define pipelines using map, filter, window, join, and custom operators
- ⚙️ **Async & Backpressure‑Aware** — built on `asyncio` to support high‑throughput event streams
- 🗂️ **Stateful Processing** — per‑key and operator‑scoped state stores
- 🔁 **Checkpointing & Recovery** — restart safely without data loss
- 📦 **Pluggable Connectors** — Kafka, MQTT, HTTP, files, and custom sources/sinks
- 🧪 **Deterministic Local Runner** — easy to test and simulate pipelines
- 🛠️ **Framework‑First Design** — opinionated core, extensible via plugins

---

## 🏗️ Example

```python
from pspf import Stream, operators as op

(
    Stream.from_kafka("inventory-events", brokers=["localhost:9092"])
        .pipe(op.key_by(lambda e: e["item_id"]))
        .pipe(op.map(lambda e: {**e, "delta": e["qty_after"] - e["qty_before"]}))
        .pipe(op.window(tumbling=60))  # 60s tumbling window
        .pipe(op.reduce(lambda acc, e: acc + e["delta"], initial=0))
        .sink.print()
)
Run the pipeline:

bash
Copy code
pspf run app.py
🚀 Getting Started
bash
Copy code
pip install pspf
Or install from source:

bash
Copy code
git clone https://github.com/<org>/python-stream-processing-framework.git
cd python-stream-processing-framework
pip install -e .
🧱 Core Concepts
Stream — a continuous sequence of events

Operator — a transformation step in a pipeline

State Store — persistent per‑key or operator state

Checkpoint — durable snapshot for recovery

Runner — executes a pipeline (local / distributed)

Full docs coming soon.

🗺️ Roadmap
 Distributed runner

 More built‑in connectors

 Exactly‑once semantics (experimental)

 CLI pipeline inspector

 Web UI for metrics & topology graph

🤝 Contributing
Contributions are welcome! Please:

Open an issue to discuss major changes

Follow the project coding style & tests

Add documentation for new features

📜 License
MIT — see LICENSE for details.

💡 Why PSPF?
PSPF focuses on clarity, composability, and Python‑first design, making it ideal for:

backend engineers building event‑driven business systems

teams prototyping stream architectures

learning modern stream‑processing concepts without heavyweight platforms
