from textwrap import dedent

import pytest
from ruamel.yaml import YAML

from pangeo_forge_runner.feedstock import Feedstock

yaml = YAML()


@pytest.fixture(params=["recipe_object", "dict_object"])
def meta_yaml(request):
    return (
        dedent(
            """\
        recipes:
          - id: aws-noaa-sea-surface-temp-whoi
            object: 'recipe:recipe'
        """  # noqa: E501
        )
        if request.param == "recipe_object"
        else dedent(
            """\
        recipes:
          dict_object: 'recipe:recipes'
        """
        )
    )


def test_feedstock(meta_yaml, tmp_path_factory: pytest.TempPathFactory):
    tmp = tmp_path_factory.mktemp("feedstock")
    with open(tmp / "meta.yaml", mode="w") as f:
        f.write(meta_yaml)

    f = Feedstock(feedstock_dir=tmp)
    assert f.meta == yaml.load(meta_yaml)
