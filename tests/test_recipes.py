from pangeo_forge_runner import Feedstock
from pathlib import Path

HERE = Path(__file__).parent

def test_recipes_list():
    list_recipe = HERE / 'test-recipes/list-recipes'
    f = Feedstock(list_recipe)
    recipes = f.parse_recipes()
    assert recipes == {
        'test_1': 'test_1',
        'test_2': 'test_2'
    }


def test_recipes_dict():
    list_recipe = HERE / 'test-recipes/list-recipes'
    f = Feedstock(list_recipe)
    recipes = f.parse_recipes()
    assert recipes == {
        'test_1': 'test_1',
        'test_2': 'test_2'
    }