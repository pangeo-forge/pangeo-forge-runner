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

        recipe = {"my_recipe": Recipe()}
        """
        )

    with open(tmpdir / "meta.yaml", mode="w") as f:
        f.write(meta_yaml)
    with open(tmpdir / "recipe.py", mode="w") as f:
        f.write(recipe_py)

    yield tmpdir, meta_yaml


def test_feedstock(tmp_feedstock):
    tmpdir, meta_yaml = tmp_feedstock
    f = Feedstock(feedstock_dir=tmpdir)
    assert f.meta == yaml.load(meta_yaml)
    # expanded_meta = f.get_expanded_meta()
    # recipes = f.parse_recipes()
