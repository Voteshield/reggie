import pytest

# To test
import reggie


def test_package_parts():
    """
    Basic smoke test to make sure the parts are there.
    """
    assert reggie.convert_cli is not None
    assert reggie.convert_voter_file is not None
