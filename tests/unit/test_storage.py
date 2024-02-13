import unittest
from dataclasses import dataclass, field
from typing import Any, Dict

import pytest

from pangeo_forge_runner.storage import TargetStorage


@dataclass
class MockFSSpecTargetv0105:
    fs: Any
    root_path: str = ""
    fsspec_kwargs: Dict[Any, Any] = field(default_factory=dict)


@dataclass
class MockFSSpecTargetv0104:
    fs: Any
    root_path: str = ""


def test_get_forge_target_with_fsspec_kwargs():
    with unittest.mock.patch.dict(
        "sys.modules", {"pangeo_forge_recipes": unittest.mock.Mock()}
    ):
        # setup
        import pangeo_forge_recipes

        pangeo_forge_recipes.storage.FSSpecTarget = MockFSSpecTargetv0105

        # act
        target_storage_instance = TargetStorage()
        result = target_storage_instance.get_forge_target("testing123")

        # assert
        assert result.fsspec_kwargs == {}


def test_get_forge_target_without_fsspec_kwargs():
    with unittest.mock.patch.dict(
        "sys.modules", {"pangeo_forge_recipes": unittest.mock.Mock()}
    ):
        # setup
        import pangeo_forge_recipes

        pangeo_forge_recipes.storage.FSSpecTarget = MockFSSpecTargetv0104

        # act
        target_storage_instance = TargetStorage()
        result = target_storage_instance.get_forge_target("testing123")

        # assert
        with pytest.raises(AttributeError):
            result.fsspec_kwargs
