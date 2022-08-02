from pangeo_forge_runner.bakery.dataflow import DataflowBakery
from traitlets import TraitError
import pytest
import shutil
import uuid
import subprocess


def test_default_gcp_project():
    """
    Test using `gcloud` to determine currently active project
    """
    # We want `gcloud` to actually be present in the environment
    assert shutil.which('gcloud')

    # We fetch the currnet project (if it exists) to make sure we restore it after our test
    current_project = None

    proc = subprocess.run(['gcloud', 'config', 'get-value', 'project'], stdout=subprocess.PIPE)
    if proc.returncode == 0:
        current_project = proc.stdout.decode().strip()

    new_project = str(uuid.uuid4())

    try:
        # Set stdin to /dev/null so `gcloud` doesn't ask us 'why are you setting project to something you can not access?'
        subprocess.run(['gcloud', 'config', 'set', 'project', new_project], check=True, stdin=subprocess.DEVNULL)
        dfb = DataflowBakery()
        assert dfb.project_id == new_project
    finally:
        if current_project:
            subprocess.run(['gcloud', 'config', 'set', 'project', current_project])


def test_temp_gcs_location_validation():
    dfb = DataflowBakery()
    with pytest.raises(TraitError):
        dfb.temp_gcs_location = 'This Should be an Error'

    dfb.temp_gcs_location = 'gs://this-should-not-error'