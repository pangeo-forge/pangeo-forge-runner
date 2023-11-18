from textwrap import dedent

import pytest
from ruamel.yaml import YAML

from pangeo_forge_runner.feedstock import Feedstock
from pangeo_forge_runner.meta_yaml import MetaYaml

yaml = YAML()


@pytest.fixture(params=["recipe_object", "dict_object", "both"])
def tmp_feedstock(request, tmp_path_factory: pytest.TempPathFactory):
    tmpdir = tmp_path_factory.mktemp("feedstock")
    if request.param == "recipe_object":
        meta_yaml = dedent(
            """\
        recipes:
          - id: aws-noaa-sea-surface-temp-whoi
            object: 'recipe:recipe'
        """
        )
        recipe_py = dedent(
            """\
        class Recipe:
          pass

        recipe = Recipe()
        """
        )
    elif request.param == "dict_object":
        meta_yaml = dedent(
            """\
        recipes:
          - dict_object: 'recipe:recipes'
        """
        )
        recipe_py = dedent(
            """\
        class Recipe:
          pass

        recipes = {"my_recipe": Recipe()}
        """
        )

    elif request.param == "both":
        meta_yaml = dedent(
            """\
        recipes:
          - id: aws-noaa-sea-surface-temp-whoi
            object: 'recipe:recipe'
          - dict_object: 'recipe:recipes'
        """
        )
        recipe_py = dedent(
            """\
        class Recipe:
          pass

        recipe = Recipe()
        recipes = {"my_recipe": Recipe()}
        """
        )

    with open(tmpdir / "meta.yaml", mode="w") as f:
        f.write(meta_yaml)
    with open(tmpdir / "recipe.py", mode="w") as f:
        f.write(recipe_py)

    yield tmpdir, meta_yaml, request.param


def test_feedstock(tmp_feedstock):
    tmpdir, meta_yaml, recipes_section_type = tmp_feedstock
    f = Feedstock(feedstock_dir=tmpdir)
    # equality of HasTraits instances doesn't work as I might expect,
    # so just check equality of the relevant trait (`.recipes`)
    assert f.meta.recipes == MetaYaml(**yaml.load(meta_yaml)).recipes

    recipes = f.parse_recipes()

    # the recipe_object metadata  looks something like this:
    #   {'recipes': [{'id': 'my_recipe', 'object': 'DICT_VALUE_PLACEHOLDER'}]}
    # and the dict_object metadata looks like this:
    #   {'recipes': [{'id': 'aws-noaa-sea-surface-temp-whoi', 'object': 'recipe:recipe'}]}
    if recipes_section_type == "recipe_object":
        expanded_meta = f.get_expanded_meta()
        assert expanded_meta["recipes"] == [
            {"id": "aws-noaa-sea-surface-temp-whoi", "object": "recipe:recipe"},
        ]
    elif recipes_section_type == "dict_object":
        expanded_meta = f.get_expanded_meta()
        assert expanded_meta["recipes"] == [
            {"id": "my_recipe", "object": "DICT_VALUE_PLACEHOLDER"},
        ]
    elif recipes_section_type == "both":
        with pytest.raises(NotImplementedError):
            _ = f.get_expanded_meta()

    for r in recipes.values():
        # the values of the recipes dict should all be python objects
        # we used the mock type `Recipe` here, so this should be true:
        assert str(r).startswith("<Recipe object")
