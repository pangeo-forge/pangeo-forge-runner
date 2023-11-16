import copy
from textwrap import dedent

import pytest
from jsonschema import ValidationError, validate
from ruamel.yaml import YAML

from pangeo_forge_runner.meta_yaml.schema import get_schema

yaml = YAML()


@pytest.fixture
def schema():
    return get_schema()


@pytest.fixture
def with_recipes_list() -> str:
    return dedent(
        """\
    title: 'AWS NOAA WHOI SST'
    description: 'Analysis-ready datasets derived from AWS NOAA WHOI NetCDF'
    pangeo_forge_version: '0.9.2'
    pangeo_notebook_version: '2021.07.17'
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
    bakery:
      id: 'pangeo-ldeo-nsf-earthcube'
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
          dict_object: 'recipe:recipes'
        """
        ),
    )
    return yaml.load(with_dict_object)


def test_schema_valid(valid_meta_yaml, schema):
    validate(valid_meta_yaml, schema=schema)


def test_schema_valid_dict_object(valid_meta_yaml_dict_object, schema):
    validate(valid_meta_yaml_dict_object, schema=schema)


@pytest.mark.parametrize(
    "field",
    [
        "title",
        "description",
        "pangeo_forge_version",
        "pangeo_notebook_version",
        "recipes",
        "provenance",
        "maintainers",
        "bakery",
    ],
)
def test_missing_toplevel_field(valid_meta_yaml, field, schema):
    invalid_meta_yaml = copy.deepcopy(valid_meta_yaml)
    del invalid_meta_yaml[field]
    with pytest.raises(ValidationError, match=f"'{field}' is a required property"):
        validate(invalid_meta_yaml, schema=schema)


@pytest.mark.parametrize(
    "subfield",
    [
        "id",
        "object",
    ],
)
def test_missing_recipes_subfield(valid_meta_yaml, subfield, schema):
    invalid_meta_yaml = copy.deepcopy(valid_meta_yaml)
    del invalid_meta_yaml["recipes"][0][subfield]

    with pytest.raises(ValidationError, match=f"'{subfield}' is a required property"):
        validate(invalid_meta_yaml, schema=schema)


@pytest.mark.parametrize(
    "subfield",
    [
        "providers",
        "license",
    ],
)
def test_missing_provenance_subfield(valid_meta_yaml, subfield, schema):
    invalid_meta_yaml = copy.deepcopy(valid_meta_yaml)
    del invalid_meta_yaml["provenance"][subfield]

    with pytest.raises(ValidationError, match=f"'{subfield}' is a required property"):
        validate(invalid_meta_yaml, schema=schema)


@pytest.mark.parametrize(
    "subfield",
    [
        "name",
        "description",
        "roles",
        "url",
    ],
)
def test_missing_providers_subfield(valid_meta_yaml, subfield, schema):
    invalid_meta_yaml = copy.deepcopy(valid_meta_yaml)
    del invalid_meta_yaml["provenance"]["providers"][0][subfield]

    with pytest.raises(ValidationError, match=f"'{subfield}' is a required property"):
        validate(invalid_meta_yaml, schema=schema)


@pytest.mark.parametrize(
    "subfield",
    [
        "name",
        "orcid",
        "github",
    ],
)
def test_missing_maintainers_subfield(valid_meta_yaml, subfield, schema):
    invalid_meta_yaml = copy.deepcopy(valid_meta_yaml)
    del invalid_meta_yaml["maintainers"][0][subfield]

    with pytest.raises(ValidationError, match=f"'{subfield}' is a required property"):
        validate(invalid_meta_yaml, schema=schema)


@pytest.mark.parametrize(
    "subfield",
    [
        "id",
    ],
)
def test_missing_bakery_subfield(valid_meta_yaml, subfield, schema):
    invalid_meta_yaml = copy.deepcopy(valid_meta_yaml)
    del invalid_meta_yaml["bakery"][subfield]

    with pytest.raises(ValidationError, match=f"'{subfield}' is a required property"):
        validate(invalid_meta_yaml, schema=schema)
