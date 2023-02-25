import logging
import os
import sys
import tempfile
from collections.abc import Generator
from contextlib import contextmanager

from pythonjsonlogger import jsonlogger
from repo2docker import contentproviders
from traitlets import Bool, Dict, Instance, List, Unicode
from traitlets.config import Application

# Common aliases we want to support in *all* commands
# The key is what the commandline argument should be, and the
# value is the traitlet config it will be translated to
common_aliases = {
    "log-level": "Application.log_level",
    "f": "BaseCommand.config_file",
    "config": "BaseCommand.config_file",
    "repo": "BaseCommand.repo",
    "ref": "BaseCommand.ref",
    "feedstock-subdir": "BaseCommand.feedstock_subdir",
}

# Common flags we want to support in *all* commands.
# The key is the name of the flag, and the value is a tuple
# consisting of a dicitonary with the traitlet config that will be
# set, and a helpful description to be printed in the commandline
common_flags = {"json": ({"BaseCommand": {"json_logs": True}}, "Generate JSON output")}


class BaseCommand(Application):
    """
    Base Application for all our subcommands.

    Provides common traitlets everyone needs, and base methods for
    fetching a given repository.

    Do not directly instantiate!
    """

    log_level = logging.INFO

    logging_config = Dict(
        {},
        config=True,
        help="""
        Logging configuration for this python application.

        When set, this value is passed to logging.config.dictConfig,
        and can be used to configure how logs *throughout the application*
        are handled, not just for logs from this application alone.

        See https://docs.python.org/3/library/logging.config.html#logging.config.dictConfig
        for more details.
        """,
    )

    repo = Unicode(
        "",
        config=True,
        help="""
        URL of feedstock repo to operate on.

        Can be anything that is interpretable by self.content_providers,
        using Repo2Docker ContentProviders. By default, this includes Git
        repos, Mercurial Repos, Zenodo, Figshare, Dataverse, Hydroshare,
        Swhid and local file paths.
        """,
    )

    ref = Unicode(
        None,
        allow_none=True,
        config=True,
        help="""
        Ref of feedstock repo to fetch.

        Optional, only used for some methods of fetching (such as git or
        mercurial)
        """,
    )

    picked_content_provider = Instance(
        klass=contentproviders.base.ContentProvider,
        config=False,
        allow_none=True,
        help="""
        Non-configurable picked content provider set by self.fetch()

        Helpful for subcommands to access the content provider in use
        """,
    )

    feedstock_subdir = Unicode(
        "feedstock",
        config=True,
        help="""
        Subdirectory inside the repository containing the `meta.yaml` file
        """,
    )

    config_file = Unicode(
        "pangeo_forge_runner_config.py",
        config=True,
        help="""
        Load traitlet config from this file if it exists
        """,
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
        help="""
        List of ContentProviders to use to fetch repo.

        Uses ContentProviders from repo2docker for doing most of the work.
        The ordering matters, and Git is used as the default for any URL
        that we can not otherwise determine.

        If we want to support additional contentproviders, ideally we can
        contribute them upstream to repo2docker.
        """,
    )

    json_logs = Bool(
        False,
        config=True,
        help="""
        Provide JSON formatted logging output to stdout.

        If set to True, *all* output will be emitted as one JSON object per
        line.

        Each line *will* have at least a 'status' field and a 'message' field.
        Various other keys will also be present based on the command being called
        and the value of 'status'.

        TODO: This *must* get a JSON schema.
        """,
    )

    @contextmanager
    def fetch(self) -> Generator[str, None, None]:
        """
        Fetch repo from configured url at ref, and return a directory where it can be accessed.

        If self.repo is a local directory that exists, it is just returned - no extra
        processing is done. If not, repo2docker is used to detect what kind of URL
        is to be checked out (git, zenodo, mercurial, etc) and that is checked out into
        a temporary directory, the path of which is returned. When the contextmanager
        exits, the temporary directory is cleaned up.
        """
        if os.path.exists(self.repo):
            # We are trying to submit off a local checkout, so we don't need to do much
            checkout_dir = self.repo

            yield checkout_dir

            # No cleanup necessary
            return
        with tempfile.TemporaryDirectory() as checkout_dir:
            for ContentProvider in self.content_providers:
                cp = ContentProvider()
                spec = cp.detect(self.repo, ref=self.ref)
                if spec is not None:
                    self.picked_content_provider = cp
                    self.log.info(
                        "Picked {cp} content "
                        "provider.\n".format(cp=cp.__class__.__name__),
                        extra={"status": "fetching"},
                    )
                    break

            if self.picked_content_provider is None:
                raise ValueError(
                    f"Could not fetch {self.repo}, no matching contentprovider found"
                )

            for log_line in self.picked_content_provider.fetch(
                spec, checkout_dir, yield_output=True
            ):
                self.log.info(log_line, extra=dict(status="fetching"))

            yield checkout_dir

    def json_excepthook(self, etype, evalue, traceback):
        """
        Called on an uncaught exception when using json logging

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
        # Load traitlets config from a config file if present
        self.load_config_file(self.config_file)

        # Allow arbitrary logging config if set
        # We do this first up so any custom logging we set up ourselves
        # is not affected, as by default dictConfig will replace all
        # existing config.
        if self.logging_config:
            logging.config.dictConfig(self.logging_config)

        # The application communicates with the outside world via
        # stdout, and we structure this communication via logging.
        # So let's setup the default logger to log to stdout, rather
        # than stderr.
        logHandler = logging.StreamHandler(sys.stdout)

        # Calling logging.getLogger gives us the *root* logger, which we
        # then futz with. Ideally, we'll call getLogger(__name__) which
        # will give us a scoped logger. Unfortunately, apache_beam doesn't
        # do this correctly and fucks with the root logger, and so must we
        # if we want to be able to control all stdout from our CLI (to be JSON or
        # otherwise). FIXME: No extra comes out here, just message
        self.log = logging.getLogger()

        # Remove all existing handlers so we don't repeat messages
        self.log.handlers = []
        self.log.addHandler(logHandler)
        self.log.setLevel(self.log_level)
        # Don't propagate these to the root logger - Apache Beam fucks with the root logger,
        # and we don't want duplicates
        self.log.propagate = False

        # Capture all warnings as well, and route them to our logger
        # This makes sure we don't accidentally write warnings to stderr
        # when calling this with --json
        # FIXME: Some extras need to be set here
        warnings_logger = logging.getLogger("py.warnings")
        warnings_logger.parent = self.log
        logging.captureWarnings(True)

        if self.json_logs:
            # register JSON excepthook to avoid non-JSON output on uncaught exception
            sys.excepthook = self.json_excepthook
            formatter = jsonlogger.JsonFormatter()
            logHandler.setFormatter(formatter)
        else:
            # Just put out the message here, nothing else.
            logHandler.formatter = logging.Formatter(fmt="%(message)s")
