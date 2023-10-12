import pytest

from pangeo_forge_runner.bakery.base import Bakery


def test_unimplemented():
    b = Bakery()
    with pytest.raises(NotImplementedError):
        b.get_pipeline_options("test", "test", {})
