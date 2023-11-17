from textwrap import dedent

import pytest
from ruamel.yaml import YAML

from pangeo_forge_runner.feedstock import Feedstock

yaml = YAML()


@pytest.fixture(params=["recipe_object", "dict_object"])
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
          dict_object: 'recipe:recipes'
        """
        )
        recipe_py = dedent(
            """\
        class Recipe:
          pass

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
    assert f.meta == yaml.load(meta_yaml)

    expanded_meta = f.get_expanded_meta()
    recipes = f.parse_recipes()

    for recipe_metadata in expanded_meta["recipes"]:
        # the recipe_object metadata  looks something like this:
        #   {'recipes': [{'id': 'my_recipe'}]}
        # and the dict_object metadata looks like this:
        #   {'recipes': [{'id': 'aws-noaa-sea-surface-temp-whoi', 'object': 'recipe:recipe'}]}
        # both have an "id" field:
        assert "id" in recipe_metadata
        # but only the "recipe_object" has an "object" field:
        if recipes_section_type == "recipe_object":
            assert "object" in recipe_metadata
        elif recipes_section_type == "dict_object":
            assert "object" not in recipe_metadata

    for r in recipes.values():
        # the values of the recipes dict should all be python objects
        # we used the mock type `Recipe` here, so this should be true:
        assert str(r).startswith("<Recipe object")
