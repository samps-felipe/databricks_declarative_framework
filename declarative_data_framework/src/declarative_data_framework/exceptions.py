"""Custom exceptions for the framework."""

class FrameworkError(Exception):
    """Base class for exceptions in this framework."""
    pass

class PipelineError(FrameworkError):
    """Raised for errors in the pipeline execution."""
    pass

class StepExecutionError(FrameworkError):
    """Raised for errors during a step execution."""
    pass

class ValidationError(StepExecutionError):
    """Raised for data validation errors."""
    pass

class ConfigurationError(FrameworkError):
    """Raised for configuration errors."""
    pass
