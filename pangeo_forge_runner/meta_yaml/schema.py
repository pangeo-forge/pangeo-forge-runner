from dataclasses import dataclass
from typing import List, Union


@dataclass
class RecipeObject:
    id: str  # TODO: require to be unique within meta.yaml namespace
    object: str  # TODO: require format '{module_name}:{recipe_instance_name}'


@dataclass
class RecipeDictObject:
    dict_object: str  # TODO: require format '{module_name}:{dict_instance_name}'


@dataclass
class Provider:
    name: str
    description: str
    roles: List[str]  # TODO: enum choices e.g. Roles.producer, Roles.licensor
    url: str


@dataclass
class Provenance:
    providers: List[Provider]
    license: str  # TODO: enum choices e.g. Licenses.cc_by_40 = "CC-BY-4.0" etc.


@dataclass
class Maintainer:
    name: str
    orcid: str  # TODO: format requirement
    github: str  # TODO: allowable characters


@dataclass
class Bakery:
    id: str  # TODO: exists in database


@dataclass
class MetaYamlMinimal:
    # TODO: this should always be a list
    # recipes: List[Union[RecipeObject, RecipeDictObject]]
    recipes: Union[List[RecipeObject], RecipeDictObject]


@dataclass
class MetaYamlExtended(MetaYamlMinimal):
    title: str
    description: str
    pangeo_forge_version: str
    pangeo_notebook_version: str
    provenance: Provenance
    maintainers: List[Maintainer]
    bakery: Bakery


def get_schema():
    from pydantic import TypeAdapter
    from pydantic.dataclasses import dataclass as pydantic_dataclass

    @pydantic_dataclass
    class MetaYamlExtendedModel(MetaYamlExtended):
        pass

    return TypeAdapter(MetaYamlExtendedModel).json_schema()
