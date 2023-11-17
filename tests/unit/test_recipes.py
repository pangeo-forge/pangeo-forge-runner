from pathlib import Path

import pytest
from ruamel.yaml import YAML
from traitlets import TraitError

from pangeo_forge_runner import Feedstock

yaml = YAML()

HERE = Path(__file__).parent


def test_recipes_list():
    list_recipe = HERE / "test-recipes/list-recipes/feedstock"
    feed = Feedstock(list_recipe)
    recipes = feed.parse_recipes()
    assert recipes == {"test_1": "test_1", "test_2": "test_2"}

    with open(list_recipe / "meta.yaml") as f:
        meta = yaml.load(f)
        assert meta == feed.get_expanded_meta()


def test_recipes_dict():
    list_recipe = HERE / "test-recipes/dict-recipes/feedstock"
    feed = Feedstock(list_recipe)
    recipes = feed.parse_recipes()
    assert recipes == {"test_1": "test_1", "test_2": "test_2"}

    with open(list_recipe / "meta.yaml") as f:
        meta = yaml.load(f)
        meta["recipes"] = [
            {"id": "test_1", "object": "DICT_VALUE_PLACEHOLDER"},
            {"id": "test_2", "object": "DICT_VALUE_PLACEHOLDER"},
        ]
        assert meta == feed.get_expanded_meta()


def test_recipes_broken():
    list_recipe = HERE / "test-recipes/broken-recipe/feedstock"
    with pytest.raises(TraitError):
        _ = Feedstock(list_recipe)
