class PipelineError(Exception):
    """Base exception for pipeline errors."""
    pass


class StageExecutionError(PipelineError):
    """Raised when an individual pipeline stage fails."""
    def __init__(self, stage_name: str, original_exception: Exception):
        super().__init__(f"Stage '{stage_name}' failed: {original_exception}")
        self.stage_name = stage_name
        self.original_exception = original_exception


class StageNotFoundError(PipelineError):
    """Raised when a requested stage handler is not registered."""
    pass
