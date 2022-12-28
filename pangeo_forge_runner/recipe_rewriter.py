"""
AST rewrites for recipe files to support injecting config
"""
from ast import Attribute, Call, Load, NodeTransformer


class RecipeRewriter(NodeTransformer):
    """
    Transform a recipe file to provide 'configuration' as needed.
    """

    def __init__(self, prune: bool = False):
        """
        prune: Set to true to add a .prune() call to FilePatterns passed to beam.Create
        """
        self.prune = prune

    def transform_prune(self, node: Call) -> Call:
        """
        Transform a FilePattern object being passed to beam.Create to call a .prune() method

        node: A ast.Call object representing the `beam.Create` call
        """
        # The object on which `.items()` is being called is what we will consider our `FilePattern` object
        # We will transform this .items() call into a .prune().items() call
        file_pattern_obj = node.args[0].func.value

        # Construct the `.prune` attribute lookup part of the new expression
        prune_attr = Attribute(
            lineno=node.lineno, col_offset=node.col_offset, attr="prune", ctx=Load()
        )
        prune_attr.value = file_pattern_obj

        # Construct the call to `.prune`
        prune_call = Call(
            lineno=node.lineno, col_offset=node.col_offset, args=[], keywords=[]
        )
        prune_call.func = prune_attr

        # Construct the `.prune().items` attribute lookup
        items_attr = Attribute(
            attr="items", lineno=node.lineno, col_offset=node.col_offset, ctx=Load()
        )
        items_attr.value = prune_call

        # Construct the `.prune().items()` call
        items_call = Call(
            lineno=node.lineno, col_offset=node.col_offset, args=[], keywords=[]
        )
        items_call.func = items_attr

        node.args = [items_call]

        return node

    def visit_Call(self, node: Call) -> Call:
        """
        Rewrite calls that return a FilePattern if we need to prune them
        """
        if not self.prune:
            return node
        if hasattr(node, "func"):
            if isinstance(node.func, Attribute):
                # We are looking for beam.Create or apache_beam.Create calls
                if node.func.attr == "Create" and (
                    node.func.value.id == "beam" or node.func.value.id == "apache_beam"
                ):
                    # If there is a single argument pased to beam.Create, and it is <something>.items()
                    # This is the heurestic we use for figuring out that we are in fact operating on a FilePattern object
                    if (
                        len(node.args) == 1
                        and isinstance(node.args[0].func, Attribute)
                        and node.args[0].func.attr == "items"
                    ):
                        return self.transform_prune(node)
        return node
