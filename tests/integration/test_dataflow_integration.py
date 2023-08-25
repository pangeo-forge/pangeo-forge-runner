import json
import subprocess
import tempfile
import time
from importlib.metadata import version

import pytest
import xarray as xr
from packaging.version import parse as parse_version


def test_dataflow_integration():
    pfr_version = parse_version(version("pangeo-forge-recipes"))
    if pfr_version >= parse_version("0.10"):
        recipe_version_ref = "0.10.x"
    else:
        recipe_version_ref = "0.9.x"
    bucket = "gs://pangeo-forge-runner-ci-testing"
    config = {
        "Bake": {
            "prune": True,
            "bakery_class": "pangeo_forge_runner.bakery.dataflow.DataflowBakery",
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
        "MetadataCacheStorage": {
            "fsspec_class": "gcsfs.GCSFileSystem",
            "root_path": bucket + "/metadata-cache/{job_name}",
        },
    }

    with tempfile.NamedTemporaryFile("w", suffix=".json") as f:
        json.dump(config, f)
        f.flush()
        cmd = [
            "pangeo-forge-runner",
            "bake",
            "--repo",
            "https://github.com/pforgetest/gpcp-from-gcs-feedstock.git",
            "--ref",
            # in the test feedstock, tags are named for the recipes version
            # which was used to write the recipe module
            recipe_version_ref,
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
