from typing import Any, Awaitable, Callable, Dict
from .exceptions import StageNotFoundError

StageHandler = Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]]


class StageRegistry:
    """Registry holding callable async handlers for workflow stages."""

    def __init__(self) -> None:
        self._handlers: Dict[str, StageHandler] = {}

    def register(self, name: str) -> Callable[[StageHandler], StageHandler]:
        """Decorator to register a stage handler by name."""
        def decorator(func: StageHandler) -> StageHandler:
            self._handlers[name] = func
            return func
        return decorator

    def get(self, name: str) -> StageHandler:
        """Retrieve a stage handler by name."""
        if name not in self._handlers:
            raise StageNotFoundError(f"Stage handler '{name}' not found.")
        return self._handlers[name]

    def has_stage(self, name: str) -> bool:
        """Check if a stage handler exists."""
        return name in self._handlers

    def list_stages(self) -> list[str]:
        """Return list of registered stage names."""
        return list(self._handlers.keys())
