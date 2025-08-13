# Testes para validações genéricas
import pytest
from declarative_data_framework.core.quality import BaseValidation, register_rule, get_validationrule

def test_register_and_get_rule():
    class DummyRule(BaseValidation):
        def apply(self, df, column_name):
            return True
    register_rule('dummy_rule', DummyRule)
    assert get_validationrule('dummy_rule') == DummyRule
