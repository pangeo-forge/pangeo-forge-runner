from unittest.mock import patch

import pytest

from pangeo_forge_runner.bakery.flink import FlinkOperatorBakery


def test_pipelineoptions():
    """
    Quickly validate some of the PipelineOptions set
    """
    fob = FlinkOperatorBakery()
    fob.parallelism = 100
    fob.max_parallelism = 100

    # FlinkOperatorBakery.get_pipeline_options calls `kubectl` in a subprocess,
    # so we patch subprocess here to skip that behavior for this test
    with patch("pangeo_forge_runner.bakery.flink.subprocess"):
        po = fob.get_pipeline_options("job", "some-container:some-tag", {})
        # some flink args, e.g. 'parallelism', are apparently 'unknown_options' from
        # the perspective of PipelineOptions, so we retain those here for the test.
        # it doesn't seem like their 'unknown' status prevents them from being passed to
        # flink in an actual deployment, though.
        opts = po.get_all_options(retain_unknown_options=True)

    assert opts["parallelism"] == 100
    assert opts["max_parallelism"] == 100


@pytest.mark.parametrize(
    "archiving_enabled, deploy_name, container_image",
    (
        [False, "archive_disabled", "apache/beam_python3.10_sdk:2.51.0"],
        [True, "archive_enabled", "apache/beam_python3.10_sdk:2.51.0"],
    ),
)
def test_make_flink_deployment(archiving_enabled, deploy_name, container_image):
    """test paths for enabled job archiving"""
    fbake = FlinkOperatorBakery()
    fbake.enable_job_archiving = archiving_enabled
    print(deploy_name, container_image)
    manifest = fbake.make_flink_deployment(deploy_name, container_image)
    if archiving_enabled:
        pod_template = manifest["spec"]["jobManager"].get("podTemplate")
        assert pod_template is not None
        for key in ["securityContext", "containers", "initContainers", "volumes"]:
            assert key in pod_template["spec"]
    else:
        pod_template = manifest["spec"]["jobManager"].get("podTemplate")
        assert pod_template is None
