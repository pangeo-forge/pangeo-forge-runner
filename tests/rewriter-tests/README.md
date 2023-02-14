# Test fixtures for RecipeRewriter

`RecipeRewriter` does AST transformations for recipe files based on
parameters passed to its constructor. This directory contains various
files that test this transformation. Each directory is a test case,
and should contain the following files:

1. `params.py` - A python file that has a dict named `params`, which will
   passed to `RecipeRewriter`'s constructor as keyword args.
2. `original.py` - The original Python recipe file.
3. `rewritten.py` - The rewritten Python recipe file.

The ASTs are compared after a roundtrip through `ast.unparse` and 
`ast.parse`. This should remove most whitespace and comment differences.
