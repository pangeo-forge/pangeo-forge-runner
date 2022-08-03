from .commands.expand_meta import ExpandMeta
from .commands.bake import Bake
from traitlets.config import Application


class App(Application):
    raise_config_file_errors = True

    subcommands = {
        'expand-meta': (ExpandMeta, "Expand meta.yaml of a config file"),
        'bake': (Bake, 'Bake a pangeo-forge recipe with a given Bakery')
    }

    def start(self):
        self.parse_command_line()
        super().start()


def main():
    app = App()
    app.start()