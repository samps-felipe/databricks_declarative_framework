# Testes para Engine genérica
import pytest
from declarative_data_framework.core.engine import BaseEngine, register_engine, get_engine

def test_register_and_get_engine():
    class DummyEngine(BaseEngine):
        def run(self):
            return 'ok'
    register_engine('dummy', DummyEngine)
    assert get_engine('dummy') == DummyEngine
