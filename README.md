# Async Pipeline Engine

A modular, lightweight asynchronous event processing and task execution library built on Python's `asyncio` standard library.

## Architecture Overview

- **`engine.py`**: Core `PipelineRunner` managing worker coroutines, concurrency semaphores, and stage execution.
- **`registry.py`**: Decorator-driven `StageRegistry` for registering async stage handlers and metadata.
- **`policies.py`**: Configurable `RetryPolicy` (exponential backoff + jitter) and `TimeoutPolicy` wrappers.
- **`hooks.py`**: Asynchronous `EventBus` and in-memory `MetricsCollector` for lifecycle telemetry.
- **`context.py`**: Structured `ExecutionContext` and `PayloadValidator` for schema and serializability checks.
- **`persistence.py`**: File-based `StateJournal` for recording stage execution outputs.
- **`cli.py`**: Command-line utility for executing sample pipelines and inspecting state journals.

## Setup & Testing

### Using `uv` (Recommended)

```bash
uv venv
source .venv/bin/activate  # On Windows Git Bash: source .venv/Scripts/activate
uv pip install -e ".[dev]"
pytest
```

### Run tests without activating
```bash
uv run --extra dev pytest
```

## CLI Usage
```bash
python -m async_pipeline.cli run --workflow etl --journal ./etl_journal.json
python -m async_pipeline.cli inspect --journal ./etl_journal.json
```

