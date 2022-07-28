import logging
from repo2docker import contentproviders
import argparse
import json
import os
import sys
import tempfile
from pathlib import Path

from pangeo_forge_runner import Feedstock

# Content providers from repo2docker are *solely* used to check out a repo
# and get their contents locally, so we can work on them.
content_providers = [
    contentproviders.Local,
    contentproviders.Zenodo,
    contentproviders.Figshare,
    contentproviders.Dataverse,
    contentproviders.Hydroshare,
    contentproviders.Swhid,
    contentproviders.Mercurial,
    contentproviders.Git,
]

logging.basicConfig(format="%(asctime)s %(msg)s", level=logging.DEBUG)
log = logging


def fetch(url, ref, checkout_path):
    """
    Fetch repo from url at ref, and check it out to checkout_path

    Uses repo2docker to detect what kinda url is going to be checked out,
    and fetches it into checkout_path. No image building or anything is
    performed.

    checkout_path should be empty.
    """
    picked_content_provider = None
    for ContentProvider in content_providers:
        cp = ContentProvider()
        spec = cp.detect(url, ref=ref)
        if spec is not None:
            picked_content_provider = cp
            log.info(
                "Picked {cp} content " "provider.\n".format(cp=cp.__class__.__name__)
            )
            break

    if picked_content_provider is None:
        raise ValueError(f'Could not fetch {url}')

    for log_line in picked_content_provider.fetch(
        spec, checkout_path, yield_output=True
    ):
        log.info(log_line, extra=dict(phase="fetching"))


def main():
    argparser = argparse.ArgumentParser()

    argparser.add_argument("--repo", help="URL to repo of feedstock to fetch")
    argparser.add_argument(
        "--ref", default=None, help="Ref to check out of the repo to build"
    )

    commands = argparser.add_subparsers(dest="command")

    expand_meta = commands.add_parser("expand-meta")
    expand_meta.add_argument(
        "--out",
        help="Path to write expanded meta.yaml out to. Will be serialized to json",
    )

    args = argparser.parse_args()

    if args.command == "expand-meta":
        with tempfile.TemporaryDirectory() as checkout_dir:
            fetch(args.repo, args.ref, checkout_dir)
            feedstock = Feedstock(Path(checkout_dir))
            expanded = feedstock.get_expanded_meta()
            with open(args.out, "w") as f:
                json.dump(expanded, f)


if __name__ == "__main__":
    main()
