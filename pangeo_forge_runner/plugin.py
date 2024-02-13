"""
Handle the plugin system for injections.

There are three parts of injections:

1. An "injection spec", provided by other installed packages (such as
   pangeo_forge_recipes, pangeo_forge_cmr, etc). This specifies what
   *values* exactly will be injected as args for *which* callables.
   It is in the form of a dictionary, and looks like this:
   ```
   {
     "<callable-1-name>": {
       "<argument-1-name>": "<value-spec>",
       "<argument-2-name>": "<value-spec>"
     },
     "<callable-2-name>": {
       "<argument-1-name>": "<value-spec>",
       "<argument-2-name>": "<value-spec>"
     }
   }
   ```

   `<value-spec>` specifies what value should be injected. Currently
   supported are two strings:
   1. `TARGET_STORAGE` - Storage that a pipeline *output* should be written to.
       Will be passed as a `pangeo_forge_recipes.storage.FSSpecTarget` object.
   2. `INPUT_CACHE_STORAGE` - (Optional) Storage used for caching inputs.
       Will be passed as a `pangeo_forge_recipes.storage.CacheFSSpecTarget` object.


   Additional values may be provided in the future.

   An example is:

   ```
    {
        'StoreToZarr': {
            'target_root': 'TARGET_STORAGE',
        },
        'OpenURLWithFSSpec': {
            'cache': 'INPUT_CACHE_STORAGE'
        }
    }
    ```

   We considered making this into an Enum, but that would have required all
   packages that provide entrypoints also *import* pangeo_forge_runner. This
   was deemed too complicating, and hence raw strings are used.

2. "Injection spec values", calculated by pangeo-forge-runner. This is simply a
   mapping of "<value-spec>" to a specific value that will be injected for
   that "<value-spec>" in this particular run. This might look like:

   ```
   {
     "TARGET_STORAGE": <A `pangeo_forge_recipes.storage.FSSpecTarget` object>,
     "INPUT_CACHE_STORAGE": <A `pangeo_forge_recipes.storage.CacheFSSpecTarget` object>
   }
   ```
3. "Injections", ready to be passed on to the rewriter! This merges (1) and (2),
   and looks like:
   ```
    {
        'StoreToZarr': {
            'target_root': <A `pangeo_forge_recipes.storage.FSSpecTarget` object>
        },
        'OpenURLWithFSSpec': {
            'cache': <A `pangeo_forge_recipes.storage.CacheFSSpecTarget` object>
        }
    }
   ```

   This is what is actually injected into the recipes in the end.
"""

# Use the backported importlib_metadata as we still support Python 3.9
# Once we're on 3.10 we can remove this dependency and use the built in
# importlib.metadata
from importlib_metadata import entry_points
from jsonschema import validate

# Schema for the dictionary returned by injection spec entrypoints
INJECTION_SPEC_SCHEMA = {
    "type": "object",
    # patternProperties to allow arbitrary keys. The first level keys represent
    # callable names.
    "patternProperties": {
        ".+": {
            "type": "object",
            # Second level keys represent attribute names in the callable, and are also arbitray.
            "patternProperties": {
                # Value of the second level keys is restricted to just these two
                ".+": {
                    "type": "string",
                    "enum": ["TARGET_STORAGE", "INPUT_CACHE_STORAGE"],
                }
            },
        }
    },
    "additionalProperties": False,
}


def get_injectionspecs_from_entrypoints():
    """
    Collection injectionspecs from installed packages.

    Looks for entrypoints defined in installed packages with the
    group "pangeo_forge_runner.injections", and calls them all in
    an undefined order. They are expected to return a dict with
    specification of what exactly should be injected where, and then
    merged together.
    """
    injection_specs = {}
    eps = entry_points(group="pangeo_forge_runner.injection_specs")
    for ep in eps:
        specs = ep.load()()
        # FIXME: This throws an exception, but user doesn't know which plugin actually
        # failed validation! provide that information
        validate(specs, schema=INJECTION_SPEC_SCHEMA)
        # FIXME: This is a shallow merge, should be a deep merge instead
        injection_specs |= specs

    if injection_specs == {}:
        # Handle the specific case of pangeo-forge-recipes==0.10.x,
        # which shipped with beam transforms that need injections, but without
        # entrypoint based injection specs.
        injection_specs = {
            "StoreToZarr": {
                "target_root": "TARGET_STORAGE",
            },
            "WriteCombinedReference": {
                "target_root": "TARGET_STORAGE",
            },
            "OpenURLWithFSSpec": {"cache": "INPUT_CACHE_STORAGE"},
        }

    return injection_specs


def get_injections(injection_spec: dict, injection_values: dict) -> dict[str, str]:
    """
    Given an injection_spec and injection_values, provide actual injections
    """
    injections = {}

    for cls, params in injection_spec.items():
        for param, target in params.items():
            if target in injection_values:
                injections.setdefault(cls, {})[param] = injection_values[target]

    return injections
