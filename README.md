# Async Pipeline Engine

A modular, lightweight asynchronous event processing and task execution library built on Python's `asyncio`.

## Features
- Dynamic stage handler registration via `@register_stage`.
- Sequential execution of pipeline workflows with error propagation.
- Basic JSON state journaling.

## Setup & Testing

### Using `uv` (Recommended)

Create a virtual environment and install dependencies in editable mode:

```bash
uv venv
source .venv/bin/activate  # On Windows Git Bash: source .venv/Scripts/activate
uv pip install -e ".[dev]"
pytest
---

### 2. Core Library (`async_pipeline/`)

#### `async_pipeline/__init__.py`
```python
"""Async Pipeline Engine package."""

from .engine import PipelineRunner, PipelineTask
from .exceptions import PipelineError, StageExecutionError
from .persistence import StateJournal
from .registry import StageRegistry

__all__ = [
    "PipelineRunner",
    "PipelineTask",
    "StageRegistry",
    "StateJournal",
    "PipelineError",
    "StageExecutionError",
]