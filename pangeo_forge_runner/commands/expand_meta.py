import json
import os
import shutil
import tempfile
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
        if os.path.exists(self.repo):
            # Trying to build a local path, so no fetching is necessary
            cleanup_after = False
            checkout_dir = self.repo
        else:
            cleanup_after = True
            checkout_dir = tempfile.gettempdir()
            self.fetch(checkout_dir)

        try:
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

        finally:
            if cleanup_after:
                shutil.rmtree(checkout_dir)
