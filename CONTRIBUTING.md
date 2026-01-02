# Contributing to PSPF

Thanks for your interest in contributing to the Python Stream Processing Framework (PSPF)!
We welcome contributions that improve stability, clarity, functionality, and developer experience.

---

## 🧩 Types of Contributions

- 🐛 Bug fixes
- 🧠 New operators or connectors
- ⚙️ Performance or reliability improvements
- 📚 Documentation & examples
- 🧪 Tests and tooling

If you're proposing a significant change, please open an issue first so we can discuss the design.

---

## 🛠️ Development Setup

```bash
git clone https://github.com/<org>/python-stream-processing-framework.git
cd python-stream-processing-framework
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
Run tests:

bash
Copy code
pytest
Format & lint:

bash
Copy code
ruff format .
ruff check .
🧱 Code Guidelines
Prefer explicit, predictable behavior

Write small, composable units

Add tests for new logic

Document public APIs & operators

✔️ Pull Request Checklist
Before opening a PR:

 Tests added or updated

 Code formatted & linted

 Docs or examples updated (if applicable)

 PR description explains what & why

🗣️ Community
Be respectful, collaborative, and constructive. PSPF is intended to be a welcoming, learning‑friendly project.

📜 License
By contributing, you agree your changes are licensed under the project license.

yaml
Copy code

---

## 🧩 **Architecture Overview (diagram + notes)**

### **High‑Level Architecture**

```text
┌──────────┐     ┌──────────┐     ┌───────────────┐     ┌──────────┐
│ Sources   │ →→ │ Pipeline │ →→ │ State / Store  │ →→ │ Sinks     │
│ (Kafka,   │     │ Operators│     │ (per‑key, op) │     │ (Kafka,  │
│ MQTT, etc)│     │ map/agg  │     │ + checkpoints │     │ DB, HTTP)│
└──────────┘     └──────────┘     └───────────────┘     └──────────┘
                         │
                         ↓
                 ┌────────────────┐
                 │ Runtime Runner │
                 │ local / dist   │
                 └────────────────┘
Core Components
Connectors — ingest & emit events

Operators — transformations (map, filter, window, reduce…)

Runtime — executes pipelines, manages concurrency & backpressure

State Stores — pluggable (memory / disk / external)

Checkpointing — durable recovery snapshots

CLI — run pipelines, inspect topology (planned)

Execution Model (current focus)
Async Python runtime (asyncio)

Cooperative scheduling

Deterministic local runner

Distributed runner planned as extension

