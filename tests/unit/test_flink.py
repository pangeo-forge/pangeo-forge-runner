from typing import Optional
from unittest.mock import patch

import pytest

from pangeo_forge_runner.bakery.flink import FlinkOperatorBakery


@pytest.mark.parametrize("parallelism, max_parallelism", [(None, None), (100, 100)])
def test_pipelineoptions(
    parallelism: Optional[int],
    max_parallelism: Optional[int],
):
    """
    Quickly validate some of the PipelineOptions set
    """
    fob = FlinkOperatorBakery()
    fob.parallelism = parallelism
    fob.max_parallelism = max_parallelism

    # FlinkOperatorBakery.get_pipeline_options calls `kubectl` in a subprocess,
    # so we patch subprocess here to skip that behavior for this test
    with patch("pangeo_forge_runner.bakery.flink.subprocess"):
        po = fob.get_pipeline_options("job", "some-container:some-tag", {})
        # some flink args, e.g. 'parallelism', are apparently 'unknown_options' from
        # the perspective of PipelineOptions, so we retain those here for the test.
        # it doesn't seem like their 'unknown' status prevents them from being passed to
        # flink in an actual deployment, though.
        opts = po.get_all_options(retain_unknown_options=True)

    assert opts["flink_version"] == "1.15"

    for optional_arg, value in dict(
        parallelism=parallelism,
        max_parallelism=max_parallelism,
    ).items():
        # if these args are not passed, we don't want them to appear in
        # the pipeline opts, so we verify here that is actually happening.
        if value is None:
            assert optional_arg not in opts
        else:
            assert opts[optional_arg] == value
