import json
from pathlib import Path

from .. import Feedstock
from ..stream_capture import redirect_stderr, redirect_stdout
from .base import BaseCommand, common_aliases, common_flags


class ExpandMeta(BaseCommand):
    """
    Application to expand meta.yaml to be fully formed.

    Will execute arbitrary code if necessary to resolve
    dict_object recipes.
    """

    aliases = common_aliases
    flags = common_flags

    def start(self):
        with self.fetch() as checkout_dir:
            feedstock = Feedstock(Path(checkout_dir) / self.feedstock_subdir)
            with redirect_stderr(self.log, {"status": "running"}), redirect_stdout(
                self.log, {"status": "running"}
            ):
                expanded = feedstock.get_expanded_meta()
            if self.json_logs:
                self.log.info(
                    "Expansion complete",
                    extra={"status": "completed", "meta": expanded},
                )
            else:
                self.log.info("Expansion complete\n", extra={"status": "completed"})
                self.log.info(json.dumps(expanded))
