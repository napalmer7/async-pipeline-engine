class PipelineError(Exception):
    """Base exception for all pipeline errors."""


class StageExecutionError(PipelineError):
    """Raised when an individual pipeline stage fails after exhausting retries."""

    def __init__(self, stage_name: str, original_exception: Exception, attempts: int = 1):
        super().__init__(
            f"Stage '{stage_name}' failed after {attempts} attempt(s): {original_exception}"
        )
        self.stage_name = stage_name
        self.original_exception = original_exception
        self.attempts = attempts


class StageNotFoundError(PipelineError):
    """Raised when a requested stage handler is not registered."""


class PipelineTimeoutError(PipelineError):
    """Raised when a stage or full pipeline exceeds its configured timeout."""


class PayloadValidationError(PipelineError):
    """Raised when a stage input or output payload fails schema validation."""
