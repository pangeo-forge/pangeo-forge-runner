from pathlib import Path
from ruamel.yaml import YAML

yaml = YAML()

class Feedstock:
    def __init__(self, feedstock_dir: Path):
        """
        met is parsed YAML from feedstock/meta.yaml
        """
        self.feedstock_dir = feedstock_dir
        with open(self.feedstock_dir / 'feedstock/meta.yaml') as f:
            self.meta = yaml.load(f)


    def _import(self, spec):
        """
        Import & return given object from recipes/ in feedstock_dir
        """
        if not hasattr(self, '_import_cache'):
            self._import_cache = {}
        module, export = spec.split(':')
        if module not in self._import_cache:
            filename = self.feedstock_dir / 'feedstock' / f'{module}.py'
            with open(filename) as f:
                ns = {}
                exec(f.read(), ns)
                self._import_cache[module] = ns

        return self._import_cache[module][export]

    def parse_recipes(self):
        recipes = {}
        recipes_config = self.meta['recipes']
        if isinstance(recipes_config, list):
            for r in recipes_config:
                recipes[r['id']] = self._import(r['object'])
        elif isinstance(recipes_config, dict):
            recipes = self._import(recipes_config['dict_object'])
        else:
            raise ValueError(f'Could not parse recipes config in meta.yaml')

        return recipes
