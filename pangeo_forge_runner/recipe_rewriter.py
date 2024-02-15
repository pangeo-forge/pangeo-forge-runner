"""
AST rewrites for recipe files to support injecting config

FIXME:

Still needs to handle a couple of additional cases.
- `from apache_beam import Create` is not handled for pruning yet. Need
  to handle `ImportFrom` statements.
- `import pangeo_forge_recipes; pangeo_forge_recipes.transforms.StoreToZarr` is not handled either
"""
from ast import (
    Attribute,
    Call,
    Constant,
    Dict,
    Import,
    Load,
    Name,
    NodeTransformer,
    fix_missing_locations,
    keyword,
)
from typing import Optional


class RecipeRewriter(NodeTransformer):
    """
    Transform a recipe file to provide 'configuration' as needed.
    """

    def __init__(
        self, prune: bool = False, callable_args_injections: Optional[dict] = None
    ):
        """
        prune: Set to true to add a .prune() call to FilePatterns passed to beam.Create
        callable_args_injections: A dict of callable names (as keys) with injected kwargs as value
        """
        self.prune = prune
        self.callable_args_injections = (
            callable_args_injections if callable_args_injections else {}
        )

        self._import_aliases = {}

    def visit_Import(self, node: Import) -> Import:
        for name in node.names:
            if name.asname:
                self._import_aliases[name.asname] = name.name
            else:
                self._import_aliases[name.name] = name.name

        return node

    def get_exec_globals(self):
        """
        Return a dict with objects to be injected into recipe while executing.

        Should be passed to `globals` of `exec` function
        """
        # This is used by our transformations to inject parameters to callables
        return {"_CALLABLE_ARGS_INJECTIONS": self.callable_args_injections}

    def transform_prune(self, node: Call) -> Call:
        """
        Transform a FilePattern object being passed to beam.Create to call a .prune() method

        node: A ast.Call object representing the `beam.Create` call
        """
        if not self.prune:
            return node
        # The object on which `.items()` is being called is what we will consider our `FilePattern` object
        # We will transform this .items() call into a .prune().items() call
        file_pattern_obj = node.args[0].func.value

        items_call = Call(
            func=Attribute(
                value=Call(
                    func=Attribute(value=file_pattern_obj, attr="prune", ctx=Load()),
                    args=[],
                    keywords=[],
                ),
                attr="items",
                ctx=Load(),
            ),
            args=[],
            keywords=[],
        )

        node.args = [items_call]

        return node

    def _make_injected_get(
        self, injected_variable: str, callable_name: str, param_name: str
    ) -> Call:
        return Call(
            func=Attribute(
                value=Call(
                    func=Attribute(
                        value=Name(id=injected_variable, ctx=Load()),
                        attr="get",
                        ctx=Load(),
                    ),
                    args=[
                        Constant(value=callable_name),
                        Dict(keys=[], values=[]),
                    ],
                    keywords=[],
                ),
                attr="get",
                ctx=Load(),
            ),
            args=[Constant(value=param_name)],
            keywords=[],
        )

    def visit_Call(self, node: Call) -> Call:
        """
        Rewrite calls that return a FilePattern if we need to prune them
        """
        if isinstance(node.func, Attribute):
            # FIXME: Support it being imported as from apache_beam import Create too
            if "apache_beam" not in self._import_aliases.values():
                # if beam hasn't been imported, don't rewrite anything
                return node

            # Only rewrite parameters to apache_beam.Create, regardless
            # of how it is imported as
            if node.func.attr == "Create" and (
                self._import_aliases.get(node.func.value.id) == "apache_beam"
            ):
                # If there is a single argument pased to beam.Create, and it is <something>.items()
                # This is the heurestic we use for figuring out that we are in fact operating on a FilePattern object
                if (
                    len(node.args) == 1
                    and isinstance(node.args[0].func, Attribute)
                    and node.args[0].func.attr == "items"
                ):
                    return fix_missing_locations(self.transform_prune(node))
        elif isinstance(node.func, Name):
            # FIXME: Support importing in other ways
            for name, params in self.callable_args_injections.items():
                if name == node.func.id:
                    node.keywords += [
                        keyword(
                            arg=k,
                            value=self._make_injected_get(
                                "_CALLABLE_ARGS_INJECTIONS", name, k
                            ),
                        )
                        for k in params
                    ]
            return fix_missing_locations(node)

        return node
