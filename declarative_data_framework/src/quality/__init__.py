"""Data Quality component registry."""
from typing import Dict, Type, Callable

# This will hold the mapping from rule name to rule class
_rule_registry: Dict[str, Type] = {}


def register_rule(name: str, cls: Type = None):
    """
    Registers or overwrites a validation rule class.
    Usage as decorator: @register_rule('rule_name')
    Usage as function: register_rule('rule_name', MyClass)
    """
    if cls is not None:
        _rule_registry[name] = cls
        return cls
    def decorator(inner_cls: Type) -> Type:
        _rule_registry[name] = inner_cls
        return inner_cls
    return decorator

def get_rule_class(name: str) -> Type:
    """Retrieves a validation rule class from the registry."""
    return _rule_registry.get(name)
