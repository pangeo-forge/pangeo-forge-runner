import shutil
import subprocess
import uuid

import pytest
from traitlets import TraitError

from pangeo_forge_runner.bakery.dataflow import DataflowBakery


@pytest.mark.parametrize(
    "property, attr_name, new_value, should_set_as_default",
    (
        ["project", "project_id", str(uuid.uuid4()), True],
        [
            "account",
            "service_account_email",
            "bot@project.iam.gserviceaccount.com",
            True,
        ],
        ["account", "service_account_email", "user@university.edu", False],
    ),
)
def test_default_gcp_props(property, attr_name, new_value, should_set_as_default):
    """
    Test using `gcloud` to determine currently active properties
    """
    # We want `gcloud` to actually be present in the environment
    assert shutil.which("gcloud")

    # We fetch the current project (if it exists) to make sure we restore it after our test
    current_value = None

    proc = subprocess.run(
        ["gcloud", "config", "get-value", property], stdout=subprocess.PIPE
    )
    if proc.returncode == 0:
        current_value = proc.stdout.decode().strip()

    try:
        # Set stdin to /dev/null so `gcloud` doesn't ask us 'why are you setting project to something you can not access?'
        subprocess.run(
            ["gcloud", "config", "set", property, new_value],
            check=True,
            stdin=subprocess.DEVNULL,
        )
        dfb = DataflowBakery()
        if should_set_as_default:
            assert getattr(dfb, attr_name) == new_value
        else:
            assert getattr(dfb, attr_name) is None
    finally:
        if current_value:
            subprocess.run(["gcloud", "config", "set", property, current_value])


def test_temp_gcs_location_validation():
    dfb = DataflowBakery()
    with pytest.raises(TraitError):
        dfb.temp_gcs_location = "This Should be an Error"

    dfb.temp_gcs_location = "gs://this-should-not-error"


def test_pipelineoptions():
    """
    Quickly validate some of the PipelineOptions set
    """
    dfb = DataflowBakery()
    dfb.project_id = "hello"
    dfb.region = "us-west1"
    dfb.machine_type = "n1-standard-2"
    dfb.use_public_ips = True
    dfb.temp_gcs_location = "gs://something"

    po = dfb.get_pipeline_options(
        "job", "some-container:some-tag", {"requirements_file": "/tmp/some-file"}
    )
    opts = po.get_all_options()
    assert opts["project"] == "hello"
    assert opts["use_public_ips"]
    assert opts["temp_location"] == "gs://something"
    assert opts["machine_type"] == "n1-standard-2"
    assert opts["region"] == "us-west1"
    assert opts["experiments"] == ["use_runner_v2"]
    assert opts["save_main_session"]
    assert opts["pickle_library"] == "cloudpickle"
    assert opts["sdk_container_image"] == "some-container:some-tag"
    assert opts["job_name"] == "job"
    assert opts["runner"] == "DataflowRunner"
    assert opts["requirements_file"] == "/tmp/some-file"


def test_required_params():
    dfb = DataflowBakery()
    dfb.project_id = None
    dfb.temp_gcs_location = "gs://test"

    with pytest.raises(ValueError):
        dfb.get_pipeline_options("test", "test", {})

    dfb.project_id = "something"
    dfb.temp_gcs_location = None

    with pytest.raises(ValueError):
        dfb.get_pipeline_options("test", "test", {})


def test_missing_gcloud(mocker):
    def find_nothing_which(cmd):
        return False

    mocker.patch("shutil.which", find_nothing_which)

    dfb = DataflowBakery()
    assert dfb.project_id is None


def test_dataflow_prime():
    """
    Validate that machine_type is not set when dataflow prime is enabled
    """
    dfb = DataflowBakery()
    dfb.use_dataflow_prime = True
    dfb.temp_gcs_location = "gs://something"

    po = dfb.get_pipeline_options(
        "job", "some-container:some-tag", {"requirements_file": "/tmp/some-file"}
    )
    opts = po.get_all_options()
    assert opts["machine_type"] is None
    assert opts["dataflow_service_options"] == ["enable_prime"]
