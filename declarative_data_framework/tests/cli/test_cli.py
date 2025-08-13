# Testes para CLI
import pytest
from click.testing import CliRunner
from declarative_data_framework.cli import cli

def test_cli_help():
    runner = CliRunner()
    result = runner.invoke(cli, ['--help'])
    assert result.exit_code == 0
    assert 'Usage' in result.output
