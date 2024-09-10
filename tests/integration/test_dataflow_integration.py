import json
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest
import xarray as xr
from packaging.version import parse as parse_version

TEST_DATA_DIR = Path(__file__).parent.parent / "test-data"
TEST_GPCP_DATA_DIR = TEST_DATA_DIR / "gpcp-from-gcs"


def test_dataflow_integration(recipes_version, beam_version):
    # just grab the version part
    recipes_version_ref = recipes_version.split("==")[1]

    # .github/workflows/dataflow.yml provides
    # `--recipes-version` arg with pytest cli call
    # but if not provided (e.g. in local runs) then alert
    if not recipes_version_ref:
        raise ValueError(
            "running these tests requires you "
            "pass `--recipes-version='<version-string>'` as a `pytest` arg"
        )

    python_version = (
        f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    )
    pfr_version = parse_version(recipes_version_ref)
    if pfr_version >= parse_version("0.10"):
        recipes_version_ref = "0.10.x"
    else:
        raise ValueError(
            f"Unsupported pfr_version: {pfr_version}. Please upgrade to 0.10 or newer."
        )

    # we need to add the versions from the CLI matrix to the requirements for tests
    with open(
        str(
            TEST_GPCP_DATA_DIR
            / f"feedstock-{recipes_version_ref}-dataflow"
            / "requirements.txt"
        ),
        "a",
    ) as f:
        for r in [recipes_version, beam_version]:
            f.write(f"{r}\n")

    bucket = "gs://pangeo-forge-runner-ci-testing"
    config = {
        "Bake": {
            "prune": True,
            "bakery_class": "pangeo_forge_runner.bakery.dataflow.DataflowBakery",
            "job_name": f"gpcp-from-gcs-py{python_version.replace('.','')}-v{''.join([str(i) for i in pfr_version.release])}",
        },
        "DataflowBakery": {"temp_gcs_location": bucket + "/temp"},
        "TargetStorage": {
            "fsspec_class": "gcsfs.GCSFileSystem",
            "root_path": bucket + "/target/{job_name}",
        },
        "InputCacheStorage": {
            "fsspec_class": "gcsfs.GCSFileSystem",
            "root_path": bucket + "/input-cache/{job_name}",
        },
    }

    with tempfile.NamedTemporaryFile("w", suffix=".json") as f:
        json.dump(config, f)
        f.flush()
        cmd = [
            "pangeo-forge-runner",
            "bake",
            "--repo",
            TEST_GPCP_DATA_DIR,
            "--feedstock-subdir",
            f"feedstock-{recipes_version_ref}-dataflow",
            "--json",
            "-f",
            f.name,
        ]
        print("\nSubmitting job...")
        submit_proc = subprocess.run(cmd, capture_output=True)
        assert submit_proc.returncode == 0
        lastline = json.loads(submit_proc.stdout.decode().splitlines()[-1])
        assert lastline["status"] == "submitted"
        job_id = lastline["job_id"]
        job_name = lastline["job_name"]
        print(f"Job submitted with {job_id = }")
        # note the start time, because certain errors on dataflow manifest as long hangs,
        # and if that's the case, we'll want to bail out of this test manually, rather than
        # wait for the the job to officially fail.
        start = time.time()

        # 6 minutes seems like an average runtime for these jobs, but being optimistic
        # let's start by waiting 5 minutes
        print(f"Waiting for 5 mins, starting at {start = }")
        time.sleep(60 * 5)

        # okay, time to start checking if the job is done
        show_job = f"gcloud dataflow jobs show {job_id} --format=json".split()
        show_job_errors = [
            "gcloud",
            "logging",
            "read",
            f'resource.type="dataflow_step" AND resource.labels.job_id="{job_id}" AND severity>=ERROR',
            "--limit",
            "500",
            "--format=json",
        ]
        while True:
            elapsed = time.time() - start
            print(f"Time {elapsed = }")
            if elapsed > 60 * 12:
                # if 12 minutes have elapsed (twice the expected time to complete the job),
                # we're going to assume the job is hanging, and call this test a failure.
                # remember: we're sourcing data for this job from within GCS, so networking
                # shouldn't delay things *too* much. if we eventually find that jobs may take
                # more than 12 minutes and not be hanging, we can change this assumption.
                pytest.fail(f"Time {elapsed = } exceedes 12 minutes.")

            # check job state
            state_proc = subprocess.run(show_job, capture_output=True)
            assert state_proc.returncode == 0
            state = json.loads(state_proc.stdout)["state"]
            print(f"Current {state = }")
            if state == "Done":
                # on Dataflow, "Done" means success
                break
            elif state == "Running":
                # still running, let's give it another 30s then check again
                time.sleep(30)
            else:
                # try to get some output to the stdout so we don't have to log into the GCP console
                state_proc = subprocess.run(show_job_errors, capture_output=True)
                assert state_proc.returncode == 0
                print(json.loads(state_proc.stdout))
                # consider any other state a failure
                pytest.fail(f"{state = } is neither 'Done' nor 'Running'")

        # open the generated dataset with xarray!
        target_path = config["TargetStorage"]["root_path"].format(job_name=job_name)
        if pfr_version >= parse_version("0.10"):
            # in pangeo-forge-eecipes>=0.10.0, an additional `StoreToZarr.store_name` kwarg
            # is appended to the formatted root path at execution time. for ref `0.10.x`,
            # the value of that kwarg is "gpcp", so we append that here.
            target_path += "/gpcp"
        gpcp = xr.open_dataset(target_path, engine="zarr")

        assert (
            gpcp.title
            == "Global Precipitation Climatatology Project (GPCP) Climate Data Record (CDR), Daily V1.3"
        )
        # --prune prunes to two time steps by default, so we expect 2 items here
        assert len(gpcp.precip) == 2
        print(gpcp)
