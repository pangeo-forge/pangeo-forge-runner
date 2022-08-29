import json
import subprocess
import tempfile

import pytest
import xarray as xr


@pytest.mark.parametrize("recipe_id", [None, "gpcp"])
def test_gpcp_bake(minio, recipe_id):
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

    with tempfile.NamedTemporaryFile("w", suffix=".json") as f:
        json.dump(config, f)
        f.flush()
        cmd = [
            "pangeo-forge-runner",
            "bake",
            "--repo",
            "https://github.com/pangeo-forge/gpcp-feedstock.git",
            "--ref",
            "2cde04745189665a1f5a05c9eae2a98578de8b7f",
            "-f",
            f.name,
        ]
        proc = subprocess.run(cmd)

        assert proc.returncode == 0

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
