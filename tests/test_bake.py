import json
import subprocess
import tempfile

import pytest
import xarray as xr


def pip_install(pkg):
    proc = subprocess.run(f"pip install -U '{pkg}'".split())
    assert proc.returncode == 0


@pytest.fixture(params=["0.9.x", "beam-refactor"])
def feedstock_ref(request):
    ref: str = request.param
    pfr = "pangeo-forge-recipes"
    before = subprocess.check_output(f"pip freeze | grep {pfr}").strip()
    if ref == "0.9.x":
        pip_install(f"{pfr}=={ref.replace('x', '*')}")
    elif ref == "beam-refactor":
        pip_install(f"https://github.com/pangeo-forge/{pfr}.git@{ref}")
    yield ref
    # for idempotence, re-install whichever version was there before the test
    pip_install(before)


@pytest.mark.parametrize(
    "recipe_id, expected_error, custom_job_name",
    (
        [None, None, None],
        ["gpcp-from-gcs", None, None],
        [
            "invalid_recipe_id",
            "ValueError: self.recipe_id='invalid_recipe_id' not in ['gpcp-from-gcs']",
            None,
        ],
        [None, None, "special-name-for-job"],
    ),
)
def test_gpcp_bake(minio, recipe_id, expected_error, custom_job_name, feedstock_ref):
    fsspec_args = {
        "key": minio["username"],
        "secret": minio["password"],
        "client_kwargs": {"endpoint_url": minio["endpoint"]},
    }

    config = {
        "Bake": {
            "prune": True,
            "bakery_class": "pangeo_forge_runner.bakery.local.LocalDirectBakery",
        },
        "TargetStorage": {
            "fsspec_class": "s3fs.S3FileSystem",
            "fsspec_args": fsspec_args,
            "root_path": "s3://gpcp/target/",
        },
        "InputCacheStorage": {
            "fsspec_class": "s3fs.S3FileSystem",
            "fsspec_args": fsspec_args,
            "root_path": "s3://gpcp/input-cache/",
        },
        "MetadataCacheStorage": {
            "fsspec_class": "s3fs.S3FileSystem",
            "fsspec_args": fsspec_args,
            "root_path": "s3://gpcp/metadata-cache/",
        },
    }

    if recipe_id:
        config["Bake"].update({"recipe_id": recipe_id})
    if custom_job_name:
        config["Bake"].update({"job_name": custom_job_name})

    with tempfile.NamedTemporaryFile("w", suffix=".json") as f:
        json.dump(config, f)
        f.flush()
        cmd = [
            "pangeo-forge-runner",
            "bake",
            "--repo",
            "https://github.com/pforgetest/gpcp-from-gcs-feedstock.git",
            "--ref",
            feedstock_ref,
            "--json",
            "-f",
            f.name,
        ]
        proc = subprocess.run(cmd, capture_output=True)
        stdout = proc.stdout.decode().splitlines()

        if expected_error:
            assert proc.returncode == 1
            stdout[-1] == expected_error

        else:
            assert proc.returncode == 0

            for line in stdout:
                if "Running job for recipe gpcp" in line:
                    job_name = json.loads(line)["job_name"]

            if custom_job_name:
                assert job_name == custom_job_name
            else:
                assert job_name.startswith("gpcp-from-gcs-")

            # Open the generated dataset with xarray!
            gpcp = xr.open_dataset(
                config["TargetStorage"]["root_path"],
                backend_kwargs={"storage_options": fsspec_args},
                engine="zarr",
            )

            assert (
                gpcp.title
                == "Global Precipitation Climatatology Project (GPCP) Climate Data Record (CDR), Daily V1.3"
            )
            # --prune prunes to two time steps by default, so we expect 2 items here
            assert len(gpcp.precip) == 2
            print(gpcp)

            # `mc` isn't the best way, but we want to display all the files in our minio
            with tempfile.TemporaryDirectory() as mcd:
                cmd = [
                    "mc",
                    "--config-dir",
                    mcd,
                    "alias",
                    "set",
                    "local",
                    minio["endpoint"],
                    minio["username"],
                    minio["password"],
                ]

                subprocess.run(cmd, check=True)

                cmd = ["mc", "--config-dir", mcd, "ls", "--recursive", "local"]
                subprocess.run(cmd, check=True)
