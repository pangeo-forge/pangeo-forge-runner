import json
from keyword import iskeyword

from traitlets import TraitType


class JSONDict(TraitType):
    """A trait that parses arbitrary JSON strings into Python objects."""

    def from_string(self, s):
        """Load value from a single string"""
        if not isinstance(s, str):
            raise TypeError(
                f"from_string expects a string, got {repr(s)} of type {type(s)}"
            )
        return json.loads(s)


class CallableArgsInjection(JSONDict):
    """A trait that parses JSON strings into Python objects, and also requires that
    the parsed object be a nested dict in which both top- and second-level keys are
    valid Python variable names (because the former must be names of callables in
    the recipe module, and the latter must be argument names on those callables).
    """

    info_text = (
        "a nested dict in which both top- and second-level keys are valid Python "
        "variable names"
    )

    @staticmethod
    def is_valid_variable_name(name: str):
        """Determine if a name is a valid Python variable name."""
        return name.isidentifier() and not iskeyword(name)

    def validate(self, obj, value):
        if (
            isinstance(value, dict)
            # top-level values must also be dicts
            and all([isinstance(value[k], dict) for k in value])
            # top-level keys must be valid python variable names
            # (because they must be callables in recipe module)
            and all([self.is_valid_variable_name(k) for k in value])
            # all second-level keys must also be valid python variable names
            # (because they must be argument names of callables in recipe module)
            and all(
                [
                    self.is_valid_variable_name(inner_key)
                    for k in value
                    for inner_key in value[k]
                ]
            )
        ):
            return value
        self.error(obj, value)
