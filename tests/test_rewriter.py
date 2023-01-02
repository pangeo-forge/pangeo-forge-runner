import ast
from pathlib import Path

import pytest

from pangeo_forge_runner.recipe_rewriter import RecipeRewriter

HERE = Path(__file__).parent


@pytest.mark.parametrize(
    ("name", "params_path", "original_path", "rewritten_path"),
    [
        (
            case.name,
            HERE / f"rewriter-tests/{case.name}/params.py",
            HERE / f"rewriter-tests/{case.name}/original.py",
            HERE / f"rewriter-tests/{case.name}/rewritten.py",
        )
        for case in (HERE / "rewriter-tests").iterdir()
        if case.is_dir() and not case.name.startswith("__")
    ],
)
def test_rewriter(name, params_path: Path, original_path: Path, rewritten_path: Path):
    params = {}

    exec(compile(params_path.read_text(), filename=params_path, mode="exec"), params)

    orig_ast = ast.parse(
        source=original_path.read_text(), filename=original_path, mode="exec"
    )
    rewritten_ast = ast.parse(
        source=rewritten_path.read_text(), filename=rewritten_path, mode="exec"
    )

    rewriter = RecipeRewriter(**params["params"])

    assert ast.unparse(rewriter.visit(orig_ast)) == ast.unparse(rewritten_ast)
