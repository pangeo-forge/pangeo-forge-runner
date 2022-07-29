from traitlets.config import Application
from traitlets import Unicode, List, Bool
from repo2docker import contentproviders
import sys
import logging
from pythonjsonlogger import jsonlogger


common_aliases = {
    'log-level': 'Application.log_level',
    'f': 'BaseCommand.config_file',
    'config': 'BaseCommand.config_file',
    'repo': 'BaseCommand.repo',
    'ref': 'BaseCommand.ref',
}

common_flags = {
    'json': (
        {'BaseCommand': {'json_logs': True}},
        "Generate JSON output"
    )
}

class BaseCommand(Application):
    """
    Base Application for all our subcommands.

    Provides common traitlets everyone needs, and base methods for
    fetching a given repository.

    Not directly instantiated!
    """
    log_level = logging.INFO

    repo = Unicode(
        "",
        config=True,
        help="""
        URL of feedstock repo to operate on.

        Can be anything that is interpretable by self.content_providers,
        using Repo2Docker ContentProviders. By default, this includes Git
        repos, Mercurial Repos, Zenodo, Figshare, Dataverse, Hydroshare,
        Swhid and local file paths.
        """
    )

    ref = Unicode(
        None,
        allow_none=True,
        config=True,
        help="""
        Ref of feedstock repo to fetch.

        Optional, only used for some methods of fetching (such as git or
        mercurial)
        """
    )

    config_file = Unicode(
        'pangeo_forge_runner_config.py',
        config=True,
        help="""
        Load traitlet config from this file if it exists
        """
    )

    # Content providers from repo2docker are *solely* used to check out a repo
    # and get their contents locally, so we can work on them.
    content_providers = List(
        None,
        [
            contentproviders.Local,
            contentproviders.Zenodo,
            contentproviders.Figshare,
            contentproviders.Dataverse,
            contentproviders.Hydroshare,
            contentproviders.Swhid,
            contentproviders.Mercurial,
            contentproviders.Git,
        ],
        config=True,
    )

    json_logs = Bool(
        False,
        config=True,
        help="""
        Provide JSON formatted logging output
        """
    )


    def fetch(self, target_path):
        """
        Fetch repo from url at ref, and check it out to checkout_path

        Uses repo2docker to detect what kinda url is going to be checked out,
        and fetches it into checkout_path. No image building or anything is
        performed.

        checkout_path should be empty.
        """
        picked_content_provider = None
        for ContentProvider in self.content_providers:
            cp = ContentProvider()
            spec = cp.detect(self.repo, ref=self.ref)
            if spec is not None:
                picked_content_provider = cp
                self.log.info(
                    "Picked {cp} content " "provider.\n".format(cp=cp.__class__.__name__),
                    extra={'status': 'fetching'}
                )
                break

        if picked_content_provider is None:
            raise ValueError(f'Could not fetch {self.repo}')

        for log_line in picked_content_provider.fetch(
            spec, target_path, yield_output=True
        ):
            self.log.info(log_line, extra=dict(status="fetching"))

    def json_excepthook(self, etype, evalue, traceback):
        """Called on an uncaught exception when using json logging

        Avoids non-JSON output on errors when using --json-logs
        """
        self.log.error(
            "Error during running: %s",
            evalue,
            exc_info=(etype, evalue, traceback),
            extra=dict(status="failed"),
        )

    def initialize(self, argv=None):
        super().initialize(argv)
        self.load_config_file(self.config_file)
        logHandler = logging.StreamHandler(sys.stdout)
        self.log = logging.getLogger("pangeo-forge-runner")
        self.log.handlers = []
        self.log.addHandler(logHandler)
        self.log.setLevel(self.log_level)

        if self.json_logs:
            # register JSON excepthook to avoid non-JSON output on errors
            sys.excepthook = self.json_excepthook
            # Need to reset existing handlers, or we repeat messages
            formatter = jsonlogger.JsonFormatter()
            logHandler.setFormatter(formatter)
        else:
            # due to json logger stuff above,
            # our log messages include carriage returns, newlines, etc.
            # remove the additional newline from the stream handler
            logHandler.terminator = ""
            # We don't want a [Repo2Docker] on all messages
            # We drop all 'extras' here as well
            logHandler.formatter = logging.Formatter(fmt="%(message)s")
