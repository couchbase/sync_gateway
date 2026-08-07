# Python Tooling & Diagnostic Utilities

This directory containing python command-line tools and diagnostic scripts is managed by `uv`.

## Main Modules

| File                  | Purpose                                                                                                                                      |
| --------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `sgcollect.py`        | Command-line tool to collect debug and state information from Couchbase Sync Gateway.                                                        |
| `password_remover.py` | Utility to strip passwords, credentials, and redact sensitive user data from Sync Gateway configuration files and URLs.                      |
| `tasks.py`            | Framework defining `Task`, `PythonTask`, and `TaskRunner` to securely run diagnostic processes, gather outputs, and manage temporary states. |

## Development & Environment Setup

These tools are configured and run via the modern **Astral Python ecosystem (`uv`)**.

- **Python Interpreter:** Pinned to Python 3.13 (`pyproject.toml` definition).
- **Virtual Environment:** Automatically managed under `.venv/`.

### Core Development Commands

- **Run all tests:**

  ```bash
  uv run pytest
  ```

- **Run type-checking (Astral `ty`):**

  ```bash
  uv run ty check
  ```


- **Lint Python files (Ruff):**

  ```bash
  uv run ruff check tools/ tools-tests/
  ```

## Testing Patterns (`tools-tests/`)

All Python unit tests reside inside the `tools-tests/` folder matching the names of the python files in tools

## Conventions

- **Type Annotations:** All main interfaces and helper functions must be fully type-annotated (`uv run ty check` must pass 100% cleanly).
- **Immutability:** Constant configurations and collections (like platform lists) should be stored in tuples (e.g. `tuple[str, ...]`) to maintain immutability.
