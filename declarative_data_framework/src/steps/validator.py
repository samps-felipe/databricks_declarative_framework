from typing import Any
from ..core.step import BaseStep
from ..core.engine import BaseEngine
from ..models.pydantic_models import PipelineConfig
from ..quality import get_rule_class
from ..exceptions import ValidationError, ConfigurationError
from ..logger import get_logger

logger = get_logger(__name__)

class ValidateStep(BaseStep):
    """
    Orchestrates the execution of column and table validations.
    """
    def _parse_rule(self, rule_string: str) -> tuple:
        """Extracts rule name and parameter. Ex: 'pattern:regex' -> ('pattern', 'regex')"""
        parts = rule_string.split(':', 1)
        return parts[0], parts[1] if len(parts) > 1 else None

    def _create_validator(self, rule_name: str, param: str):
        """Creates a validator instance using the registry, passing all parameters as a dict."""
        rule_class = get_rule_class(rule_name)
        if not rule_class:
            logger.warning(f"Unknown validation rule '{rule_name}'. Skipping.")
            return None
        try:
            params = {}
            if param:
                # Always pass the parameter as a dict with a generic key
                params = {f"{rule_name}_param": param}
                # For backward compatibility, also pass param with common keys
                params["pattern"] = param
                params["allowed_values_str"] = param
                params["value_str"] = param
                params["bounds_str"] = param
            return rule_class(params)
        except (TypeError, ValueError) as e:
            raise ConfigurationError(f"Failed to instantiate rule '{rule_name}' with param '{param}'. Error: {e}") from e

    def execute(self, df, engine: BaseEngine, config: PipelineConfig) -> tuple:
        logger.info("--- Step: Validation ---")
        return engine.validate(df, config)
