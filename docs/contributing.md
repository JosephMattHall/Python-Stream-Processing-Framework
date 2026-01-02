🧩 Contributing Guide — Python Stream Processing Framework (PSPF)
Thank you for contributing to PSPF! This project aims to provide a clear, reliable, and extensible Python stream‑processing framework. Contributions should emphasize readability, correctness, and long‑term maintainability.

📌 Contribution Process
Open an Issue before starting significant work

Describe the problem / feature

Propose an approach or design

Create a feature branch

git checkout -b feature/<short‑description>
Write code + tests + docs

Submit a PR with a concise description of what and why

Be open to review feedback — we value discussion and clarity

🧱 Code Style & Quality
We follow PEP 8, modern Python conventions, and consistent structure.

Python 3.11+

Use type hints everywhere practical

Use meaningful names — prefer clarity over cleverness

Keep functions focused and cohesive

Avoid unnecessary abstractions unless they add value

Formatting & Tools
The project uses:

ruff — linting & fixes

black — formatting

mypy — type checking

pytest — testing

Before committing:

ruff check .
black .
mypy .
pytest
PRs should pass all checks.

✍️ Comments & Docstrings
Write comments like an experienced developer:

Explain why decisions were made, not what the code already says

Keep comments short and relevant

Add TODOs when intentionally deferring work

Use docstrings for:

Public classes & methods

Operator interfaces

Extension points

Prefer Google‑ or NumPy‑style docstrings.

🧪 Testing Guidelines
Add tests for new functionality

Prefer small, focused tests

Use deterministic, repeatable behavior

When appropriate, mock external I/O

Tests go in tests/ and should run with:

pytest
📚 Documentation
Update relevant docs when behavior or APIs change

Include examples when introducing new operators or concepts

Keep README examples runnable

Architecture docs should describe design intent, not implementation trivia.

🔀 Git & Commit Conventions
Use clear, meaningful commit messages:

feat: add windowed reduce operator
fix: handle checkpoint retry edge case
refactor: simplify pipeline state lifecycle
docs: expand runtime architecture section
tests: add coverage for FileSource batching
Keep PRs small and focused where possible.

🧩 Design Principles
Contributions should align with these values:

Simplicity over complexity

Explicit over implicit

Extensible operator model

Predictable runtime behavior

Minimal external dependencies

Performance improvements must not reduce clarity without justification

🤝 Code Review Philosophy
Reviews aim to improve:

correctness

clarity

maintainability

consistency

Disagreement is fine — provide reasoning and be respectful.

🏷️ Labels & Contribution Types
Common contribution types:

core-runtime

operators

state/checkpoints

io-sources

io-sinks

docs

tests

refactor

Tag issues and PRs accordingly when possible.

🧭 Roadmap Contributions
If your change affects architecture or direction:

Start with a design proposal

Discuss trade‑offs

Align with project goals before implementation