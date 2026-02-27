"""Tests for knode CLI."""

import pytest
from click.testing import CliRunner

from knode.cli import main


def test_cli_help():
    """CLI --help exits 0 and shows usage."""
    runner = CliRunner()
    result = runner.invoke(main, ["--help"])
    assert result.exit_code == 0
    assert "knode" in result.output
    assert "list" in result.output or "cordon" in result.output
