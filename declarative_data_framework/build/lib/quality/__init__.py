"""Data Quality component registry."""
from typing import Dict, Type, Callable

# This will hold the mapping from rule name to rule class
_rule_registry: Dict[str, Type] = {}

def register_rule(name: str) -> Callable:
    """A decorator to register a validation rule class."""
    def decorator(cls: Type) -> Type:
        _rule_registry[name] = cls
        return cls
    return decorator

def get_rule_class(name: str) -> Type:
    """Retrieves a validation rule class from the registry."""
    return _rule_registry.get(name)
