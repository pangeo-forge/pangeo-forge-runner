import hashlib
import json
import re
import subprocess
import sys
import tempfile
from importlib.metadata import distributions, version
from pathlib import Path

import pytest
import xarray as xr
from packaging.version import parse as parse_version

from pangeo_forge_runner.commands.bake import Bake

TEST_DATA_DIR = Path(__file__).parent.parent / "test-data"


@pytest.fixture
def recipes_uninstalled():
    """Uninstall `pangeo-forge-recipes` for `test_bake_requires_recipes_installed`."""
    # first confirm that it's installed to begin with
    assert "pangeo-forge-recipes" in [d.metadata["Name"] for d in distributions()]
    # and capture the version, which we'll reinstall after the test
    recipes_version = parse_version(version("pangeo-forge-recipes"))
    # now uninstall it
    uninstall = subprocess.run(
        f"{sys.executable} -m pip uninstall pangeo-forge-recipes -y".split()
    )
    assert uninstall.returncode == 0
    assert "pangeo-forge-recipes" not in [d.metadata["Name"] for d in distributions()]
    # and yield to the test
    yield True
    # test is complete, now reinstall pangeo-forge-recipes in the test env
    reinstall = subprocess.run(
        f"{sys.executable} -m pip install pangeo-forge-recipes=={recipes_version}".split()
    )
    assert reinstall.returncode == 0
    # make sure it's there, and in the expected version
    assert "pangeo-forge-recipes" in [d.metadata["Name"] for d in distributions()]
    assert parse_version(version("pangeo-forge-recipes")) == recipes_version


def test_bake_requires_recipes_installed(recipes_uninstalled):
    """`pangeo-forge-runner` does not require `pangeo-forge-recipes` to be installed,
    but `pangeo-forge-recipes` *is* required to use the `bake` command, so test that
    we get a descriptive error if we try to invoke this command without it installed.
    """
    assert recipes_uninstalled
    bake = Bake()
    with pytest.raises(
        ValueError,
        match="To use the `bake` command, `pangeo-forge-recipes` must be installed.",
    ):
        bake.start()


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
    "container_image, raises",
    (
        ["", True],
        ["apache/beam_python3.10_sdk:2.51.0", False],
    ),
)
def test_container_name_validation(container_image, raises):
    bake = Bake()
    if raises:
        with pytest.raises(
            ValueError,
            match=r"^'container_name' is required.*",
        ):
            bake.bakery_class = "pangeo_forge_runner.bakery.flink.FlinkOperatorBakery"
            bake.container_image = container_image
    else:
        bake.bakery_class = "pangeo_forge_runner.bakery.flink.FlinkOperatorBakery"
        bake.container_image = container_image
        assert bake.container_image == container_image


@pytest.fixture(params=["recipe_object", "dict_object"])
def recipes_version_ref(request):
    pfr_version = parse_version(version("pangeo-forge-recipes"))
    if pfr_version >= parse_version("0.10"):
        recipes_version_ref = "0.10.x"
    else:
        raise ValueError(
            f"Unsupported pfr_version: {pfr_version}. Please upgrade to 0.10 or newer."
        )
    return (
        recipes_version_ref
        if not request.param == "dict_object"
        else f"{recipes_version_ref}-dictobj"
    )


@pytest.mark.parametrize(
    ("recipe_id", "expected_error", "custom_job_name", "no_input_cache"),
    (
        [None, None, None, False],
        ["gpcp-from-gcs", None, None, False],
        [
            "invalid_recipe_id",
            "ValueError: self.recipe_id='invalid_recipe_id' not in ['gpcp-from-gcs']",
            None,
            False,
        ],
        [None, None, "special-name-for-job", False],
        [None, None, None, True],
    ),
)
def test_gpcp_bake(
    minio,
    recipe_id,
    expected_error,
    custom_job_name,
    no_input_cache,
    recipes_version_ref,
):
    if recipes_version_ref == "0.10.x-dictobj" and recipe_id:
        pytest.skip(
            "We only test dictobjs for recipes >0.10.0, and without recipe_id's"
        )

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
    }

    if no_input_cache:
        config["InputCacheStorage"] = {
            "fsspec_class": "fsspec.AbstractFileSystem",
            "fsspec_args": {},
            "root_path": "",
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
            str(TEST_DATA_DIR / "gpcp-from-gcs"),
            "--feedstock-subdir",
            f"feedstock-{recipes_version_ref}",
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

            job_name_logs = [
                json.loads(line) for line in stdout if "Running job for recipe " in line
            ]
            job_names = {line["recipe"]: line["job_name"] for line in job_name_logs}
            for recipe_name, job_name in job_names.items():
                if custom_job_name:
                    assert job_name.startswith(custom_job_name)
                else:
                    assert job_name.startswith("local-gpcp-2dfrom-2dgcs-feedstock-")

                if "dictobj" in recipes_version_ref:
                    assert job_name.endswith(
                        hashlib.sha256(recipe_name.encode()).hexdigest()[:5]
                    )

            # In pangeo-forge-recipes>=0.10.0, the actual zarr store is produced in a
            # *subpath* of target_storage.rootpath, rather than in the
            # root path itself. This is a compatibility break vs the previous
            # versions of pangeo-forge-recipes. https://github.com/pangeo-forge/pangeo-forge-recipes/pull/495
            # has more information

            if recipes_version_ref == "0.10.x":
                zarr_store_full_paths = [config["TargetStorage"]["root_path"] + "gpcp/"]
            elif recipes_version_ref == "0.10.x-dictobj":
                zarr_store_root_path = config["TargetStorage"]["root_path"]
                zarr_store_full_paths = [
                    zarr_store_root_path + store_name
                    for store_name in ["gpcp-dict-key-0", "gpcp-dict-key-1"]
                ]
            else:
                zarr_store_full_paths = [config["TargetStorage"]["root_path"]]

            # dictobj runs do not generate any datasets b/c they are not recipes
            # so we've asserted what we can already, just move on
            if recipes_version_ref.endswith("dictobj"):
                return

            # Open the generated datasets with xarray!
            for path in zarr_store_full_paths:
                print(f"Opening dataset for {path = }")
                ds = xr.open_dataset(
                    # We specify a store_name of "gpcp" in the test recipe
                    path,
                    backend_kwargs={"storage_options": fsspec_args},
                    engine="zarr",
                )

                assert (
                    ds.title
                    == "Global Precipitation Climatatology Project (GPCP) Climate Data Record (CDR), Daily V1.3"
                )
                # --prune prunes to two time steps by default, so we expect 2 items here
                assert len(ds.precip) == 2
                print(ds)

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
