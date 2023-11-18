import jsonschema
from traitlets import Dict, HasTraits, List, TraitError, Unicode, validate

recipes_field_per_element_schema = {
    "type": "object",
    "properties": {
        "id": {"type": "string"},
        "object": {"type": "string"},
        "dict_object": {"type": "string"},
    },
    "oneOf": [
        {"required": ["id", "object"]},
        {"required": ["dict_object"]},
    ],
}


class MetaYaml(HasTraits):
    """Schema for the ``meta.yaml`` file which must be included in each feedstock directory.
    Only the ``recipes`` field is strictly required for ``pangeo-forge-runner`` to function.
    All other fields are recommended but not required.
    """

    def __init__(self, recipes=None, **kwargs):
        """The only required field is ``recipes``, so we put it explicitly in the init
        signature to ensure it is not omitted, as demonstrated in:
        https://github.com/ipython/traitlets/issues/490#issuecomment-479716288
        """
        super().__init__(**kwargs)
        self.recipes = recipes

    @validate("recipes")
    def _validate_recipes(self, proposal):
        """Ensure the ``recipes`` trait is not passed as an empty container and that
        each element of the field contains all expected subfields.
        """
        if not proposal["value"]:
            raise TraitError(
                f"The ``recipes`` trait, passed as {proposal['value']}, cannot be empty."
            )

        if isinstance(proposal["value"], list):
            for recipe_spec in proposal["value"]:
                try:
                    jsonschema.validate(recipe_spec, recipes_field_per_element_schema)
                except jsonschema.ValidationError as e:
                    raise TraitError(e)
        return proposal["value"]

    title = Unicode(
        allow_none=True,
        help="""
        Title for this dataset.
        """,
    )
    description = Unicode(
        allow_none=True,
        help="""
        Description of the dataset.
        """,
    )
    recipes = List(
        Dict(),
        help="""
        Specifies the deployable Python objects to run in the recipe module.
        If the recipes are assigned to their own Python variable names,
        should be of the form:

        ```yaml
        recipes:
          - id: "unique-identifier-for-recipe"
            object: "recipe:transforms"
        ```
        
        Alternatively, if the recipes are values in a Python dict:

        ```yaml
        recipes:
          - dict_object: "recipe:name_of_recipes_dict"
        ```
        """,
    )
    provenance = Dict(
        allow_none=True,
        help="""
        Dataset provenance information including provider, license, etc.
        """,
        per_key_traits={
            "providers": List(
                Dict(
                    per_key_traits={
                        "name": Unicode(),
                        "description": Unicode(),
                        "roles": List(),  # TODO: enum
                        "url": Unicode(),
                    },
                ),
            ),
            "license": Unicode(),  # TODO: guidance on suggested formatting (enum?)
        },
    )
    maintainers = List(
        Dict(
            per_key_traits={
                "name": Unicode(help="Full name of the maintainer."),
                "orcid": Unicode(help="Maintainer's ORCID ID"),  # TODO: regex
                "github": Unicode(help="Maintainer's GitHub username."),
            },
        ),
        allow_none=True,
        help="""
        Maintainers of this Pangeo Forge feedstock.
        """,
    )
