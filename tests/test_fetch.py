import os
import tempfile

import pytest
from repo2docker import contentproviders

from pangeo_forge_runner.commands.base import BaseCommand


def test_bad_cp():
    """
    Test we throw a useful exception if we can't get a ContentProvider

    This doesn't happen with the default configuration as we assume
    any URL is actually a git URL if we can't prove otherwise
    """
    bc = BaseCommand()
    bc.content_providers.remove(contentproviders.Git)

    bc.repo = "https://example.com"
    with pytest.raises(ValueError):
        with bc.fetch() as _:
            pass


def test_local():
    """
    Test that local directories are just treated as nops
    """
    with tempfile.TemporaryDirectory() as d:
        bc = BaseCommand()
        bc.repo = d

        with bc.fetch() as checkout_dir:
            assert checkout_dir == d

        # Make sure the directory isn't deleted either by the end!
        assert os.path.exists(d)
