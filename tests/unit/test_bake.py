import json
import re
import subprocess
import tempfile
from importlib.metadata import version

import pytest
import xarray as xr
from packaging.version import parse as parse_version

from pangeo_forge_runner.commands.bake import Bake


@pytest.mark.parametrize(
    "job_name, raises",
    (
        ["valid-job", False],
        ["valid_job", False],
        ["".join(["a" for i in range(63)]), False],  # <= 63 chars allowed
        ["".join(["a" for i in range(64)]), True],  # > 63 chars not allowed
        ["invali/d", True],  # dashes are the only allowable punctuation
        ["1valid-job", True],  # can only start with letters
        ["-valid-job", True],  # can only start with letters
        ["Valid-Job", True],  # uppercase letters not allowed
    ),
)
def test_job_name_validation(job_name, raises):
    bake = Bake()
    if raises:
        with pytest.raises(
            ValueError,
            match=re.escape(
                f"job_name must match the regex ^[a-z][-_0-9a-z]{{0,62}}$, instead found {job_name}"
            ),
        ):
            bake.job_name = job_name
    else:
        bake.job_name = job_name
        assert bake.job_name == job_name


@pytest.mark.parametrize(
    ("recipe_id", "expected_error", "custom_job_name"),
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
def test_gpcp_bake(minio, recipe_id, expected_error, custom_job_name):
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

    pfr_version = parse_version(version("pangeo-forge-recipes"))
    if pfr_version >= parse_version("0.10"):
        recipe_version_ref = "0.10.x"
    else:
        recipe_version_ref = "0.9.x"

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
                assert job_name.startswith("gh-pforgetest-gpcp-from-gcs-")

            # In pangeo-forge-recipes>=0.10.0, the actual zarr store is produced in a
            # *subpath* of target_storage.rootpath, rather than in the
            # root path itself. This is a compatibility break vs the previous
            # versions of pangeo-forge-recipes. https://github.com/pangeo-forge/pangeo-forge-recipes/pull/495
            # has more information

            if pfr_version >= parse_version("0.10"):
                zarr_store_path = config["TargetStorage"]["root_path"] + "gpcp/"
            else:
                zarr_store_path = config["TargetStorage"]["root_path"]
            # Open the generated dataset with xarray!
            gpcp = xr.open_dataset(
                # We specify a store_name of "gpcp" in the test recipe
                zarr_store_path,
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
