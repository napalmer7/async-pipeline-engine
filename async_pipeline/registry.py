from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Optional
from .context import PayloadValidator
from .exceptions import StageNotFoundError
from .policies import RetryPolicy

StageHandler = Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]]


@dataclass
class StageDefinition:
    """Metadata and callable wrapper for a registered pipeline stage."""

    name: str
    handler: StageHandler
    retry_policy: Optional[RetryPolicy] = None
    input_validator: Optional[PayloadValidator] = None
    description: str = ""


class StageRegistry:
    """Registry holding callable async handlers and policies for workflow stages."""

    def __init__(self) -> None:
        self._stages: Dict[str, StageDefinition] = {}

    def register(
        self,
        name: str,
        retry_policy: Optional[RetryPolicy] = None,
        input_validator: Optional[PayloadValidator] = None,
        description: str = "",
    ) -> Callable[[StageHandler], StageHandler]:
        """Decorator to register a stage handler with optional policies."""

        def decorator(func: StageHandler) -> StageHandler:
            self._stages[name] = StageDefinition(
                name=name,
                handler=func,
                retry_policy=retry_policy,
                input_validator=input_validator,
                description=description,
            )
            return func

        return decorator

    def get_definition(self, name: str) -> StageDefinition:
        """Retrieve the full StageDefinition by name."""
        if name not in self._stages:
            raise StageNotFoundError(f"Stage handler '{name}' not found.")
        return self._stages[name]

    def get(self, name: str) -> StageHandler:
        """Retrieve a stage handler callable by name."""
        return self.get_definition(name).handler

    def has_stage(self, name: str) -> bool:
        """Check if a stage handler exists."""
        return name in self._stages

    def list_stages(self) -> list[str]:
        """Return list of registered stage names."""
        return list(self._stages.keys())
