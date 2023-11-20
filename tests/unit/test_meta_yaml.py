import copy
from textwrap import dedent

import pytest
from ruamel.yaml import YAML
from traitlets import TraitError

from pangeo_forge_runner.meta_yaml import MetaYaml

yaml = YAML()


@pytest.fixture
def with_recipes_list() -> str:
    return dedent(
        """\
    title: 'AWS NOAA WHOI SST'
    description: 'Analysis-ready datasets derived from AWS NOAA WHOI NetCDF'
    recipes:
      - id: aws-noaa-sea-surface-temp-whoi
        object: 'recipe:recipe'
    provenance:
      providers:
        - name: 'AWS NOAA Oceanic CDR'
          description: 'Registry of Open Data on AWS National Oceanographic & Atmospheric Administration National Centers for Environmental Information'
          roles:
            - producer
            - licensor
          url: s3://noaa-cdr-sea-surface-temp-whoi-pds/
      license: 'Open Data'
    maintainers:
      - name: 'Jo Contributor'
        orcid: '0000-0000-0000-0000'
        github: jocontributor123
    """  # noqa: E501
    )


@pytest.fixture
def valid_meta_yaml(with_recipes_list: str) -> dict:
    return yaml.load(with_recipes_list)


@pytest.fixture
def valid_meta_yaml_dict_object(with_recipes_list: str) -> dict:
    with_dict_object = with_recipes_list.replace(
        dedent(
            """\
        recipes:
          - id: aws-noaa-sea-surface-temp-whoi
            object: 'recipe:recipe'
        """
        ),
        dedent(
            """\
        recipes:
          - dict_object: 'recipe:recipes'
        """
        ),
    )
    return yaml.load(with_dict_object)


def test_schema_valid(valid_meta_yaml):
    _ = MetaYaml(**valid_meta_yaml)


def test_schema_valid_dict_object(valid_meta_yaml_dict_object):
    _ = MetaYaml(**valid_meta_yaml_dict_object)


@pytest.mark.parametrize(
    "field",
    [
        "title",
        "description",
        "recipes",
        "provenance",
        "maintainers",
    ],
)
def test_missing_toplevel_field(valid_meta_yaml, field):
    meta_yaml_copy = copy.deepcopy(valid_meta_yaml)
    del meta_yaml_copy[field]
    if field == "recipes":
        # ``recipes`` is the only required field
        with pytest.raises(TraitError):
            _ = MetaYaml(**meta_yaml_copy)
    else:
        # all others fields can be left empty without raising an error
        _ = MetaYaml(**meta_yaml_copy)


@pytest.mark.parametrize(
    "subfield",
    [
        "id",
        "object",
    ],
)
def test_missing_recipes_subfield(valid_meta_yaml, subfield):
    invalid_meta_yaml = copy.deepcopy(valid_meta_yaml)
    del invalid_meta_yaml["recipes"][0][subfield]

    with pytest.raises(TraitError):
        _ = MetaYaml(**invalid_meta_yaml)


def test_recipes_field_cannot_be_empty_container():
    with pytest.raises(TraitError):
        _ = MetaYaml(recipes=[])


def test_recipes_field_invalid_keys():
    with pytest.raises(TraitError):
        # "dict_object" key can't be used in combination with other keys
        _ = MetaYaml(recipes={"id": "abcdefg", "dict_object": "abc:def"})
    with pytest.raises(TraitError):
        # the only valid keys are {"id", "object"} together,
        # or "dict_object" alone. other keys are not allowed.
        _ = MetaYaml(recipes={"some_other_key": "abc:def"})


# TODO: In a future "strict" mode, ensure provenance fields are all provided.
# --------------------------------------------------------------------------
# @pytest.mark.parametrize(
#     "subfield",
#     [
#         "providers",
#         "license",
#     ],
# )
# def test_missing_provenance_subfield(valid_meta_yaml, subfield, schema):
#     invalid_meta_yaml = copy.deepcopy(valid_meta_yaml)
#     del invalid_meta_yaml["provenance"][subfield]
#
#     with pytest.raises(TraitError):
#         _ = MetaYaml(**meta_yaml_copy)


# TODO: In a future "strict" mode, ensure providers fields are all provided.
# --------------------------------------------------------------------------
# @pytest.mark.parametrize(
#     "subfield",
#     [
#         "name",
#         "description",
#         "roles",
#         "url",
#     ],
# )
# def test_missing_providers_subfield(valid_meta_yaml, subfield, schema):
#     invalid_meta_yaml = copy.deepcopy(valid_meta_yaml)
#     del invalid_meta_yaml["provenance"]["providers"][0][subfield]
#
#     with pytest.raises(ValidationError, match=f"'{subfield}' is a required property"):
#         _ = MetaYaml(**meta_yaml_copy)


# TODO: In a future "strict" mode, ensure maintainers fields are all provided.
# ----------------------------------------------------------------------------
# @pytest.mark.parametrize(
#     "subfield",
#     [
#         "name",
#         "orcid",
#         "github",
#     ],
# )
# def test_missing_maintainers_subfield(valid_meta_yaml, subfield, schema):
#     invalid_meta_yaml = copy.deepcopy(valid_meta_yaml)
#     del invalid_meta_yaml["maintainers"][0][subfield]
#
#     with pytest.raises(TraitError):
#         _ = MetaYaml(**meta_yaml_copy)
