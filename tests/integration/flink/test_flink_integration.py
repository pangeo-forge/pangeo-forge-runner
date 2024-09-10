import json
import subprocess
import tempfile
import time
from pathlib import Path

import xarray as xr
from packaging.version import parse as parse_version

TEST_DATA_DIR = Path(__file__).parent.parent.parent / "test-data"
TEST_GPCP_DATA_DIR = TEST_DATA_DIR / "gpcp-from-gcs"


def test_flink_bake(
    minio_service, flink_version, python_version, beam_version, recipes_version
):
    # just grab the version part
    recipes_version_ref = recipes_version.split("==")[1]
    beam_version_ref = beam_version.split("==")[1]

    # .github/workflows/flink.yml provides
    # `--recipes-version` arg with pytest cli call
    # but if not provided (e.g. in local runs) then alert
    if not recipes_version_ref:
        raise ValueError(
            "running these tests requires you "
            "pass `--recipes-version='<version-string>'` as a `pytest` arg"
        )

    fsspec_args = {
        "key": minio_service["username"],
        "secret": minio_service["password"],
        "client_kwargs": {"endpoint_url": minio_service["endpoint"]},
    }

    pfr_version = parse_version(recipes_version_ref)
    if pfr_version >= parse_version("0.10"):
        recipes_version_ref = "0.10.x"

    # we need to add the versions from the CLI matrix to the requirements for tests
    with open(
        str(
            TEST_GPCP_DATA_DIR
            / f"feedstock-{recipes_version_ref}-flink"
            / "requirements.txt"
        ),
        "a",
    ) as f:
        for r in [recipes_version, beam_version]:
            f.write(f"{r}\n")

    bucket = "s3://gpcp-out"
    config = {
        "Bake": {
            "prune": True,
            "job_name": "recipe",
            "bakery_class": "pangeo_forge_runner.bakery.flink.FlinkOperatorBakery",
            # there must be a job-server jar available for the matching
            # `apache-beam` and `FlinkOperatorBakery.flink_version` here:
            # https://repo.maven.apache.org/maven2/org/apache/beam/beam-runners-flink-1.16-job-server/
            "container_image": f"apache/beam_python{python_version}_sdk:{beam_version_ref}",
        },
        "TargetStorage": {
            "fsspec_class": "s3fs.S3FileSystem",
            "fsspec_args": fsspec_args,
            "root_path": bucket + "/target/{job_name}",
        },
        "InputCacheStorage": {
            "fsspec_class": "s3fs.S3FileSystem",
            "fsspec_args": fsspec_args,
            "root_path": bucket + "/input-cache/{job_name}",
        },
        "FlinkOperatorBakery": {
            "flink_version": flink_version,
            "job_manager_resources": {"memory": "1024m", "cpu": 0.30},
            "task_manager_resources": {"memory": "2048m", "cpu": 0.30},
            "parallelism": 1,
            "flink_configuration": {
                "taskmanager.numberOfTaskSlots": "1",
                "taskmanager.memory.jvm-overhead.max": "2048m",
            },
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
            f"feedstock-{recipes_version_ref}-flink",
            "-f",
            f.name,
        ]

        print("\nSubmitting job...")
        timeout = 60 * 4
        with subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        ) as proc:
            start = time.time()
            for line in proc.stdout:
                # nice to have output
                print(line, end="")

                elapsed_time = time.time() - start
                if elapsed_time >= timeout:
                    raise Exception("timeout reached, exiting")

        # make sure the last time submitted job
        assert line.startswith("Started Flink job as")

        # use minio cli to inuit when job is finished after a waiting period
        # TODO: we need to get the historyserver up so we can query job status async
        # https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/deployment/advanced/historyserver/
        time.sleep(60 * 2)
        cmd = [
            "mc",
            "alias",
            "set",
            "myminio",
            minio_service["endpoint"],
            minio_service["username"],
            minio_service["password"],
        ]
        proc = subprocess.run(cmd, capture_output=True)
        assert proc.returncode == 0

        # set up path lookups for minio cli and xarray
        target_path = config["TargetStorage"]["root_path"].format(
            job_name=config["Bake"]["job_name"]
        )
        if pfr_version >= parse_version("0.10"):
            # in pangeo-forge-recipes>=0.10.0, an additional `StoreToZarr.store_name` kwarg
            # is appended to the formatted root path at execution time. for ref `0.10.x`,
            # the value of that kwarg is "gpcp", so we append that here.
            target_path += "/gpcp"

        cmd = [
            "mc",
            "ls",
            "myminio/{}/precip".format(target_path.replace("s3://", "")),
        ]
        timeout = 60 * 5
        start = time.time()
        print("[ RUNNING ]: ", " ".join(cmd))
        while True:
            proc = subprocess.run(cmd, capture_output=True, text=True)
            # purposely don't check proc.returncode since files might not exist yet

            # --prune prunes to two time steps by default, so we expect 2 time steps here
            # but four overall files:
            #
            # $ mc ls myminio/gpcp/target/recipe/gpcp/precip/
            # [2023-10-24 22:42:16 UTC]   365B STANDARD .zarray
            # [2023-10-24 22:42:16 UTC]   442B STANDARD .zattrs
            # [2023-10-24 22:42:17 UTC] 145KiB STANDARD 0.0.0
            # [2023-10-24 22:42:17 UTC] 148KiB STANDARD 1.0.0
            try:
                output = proc.stdout.splitlines()
                print(f"[ MINIO OUTPUT ]: {output[-1]}")
                if len(output) == 4:
                    break
            except:
                pass

            elapsed_time = time.time() - start
            if elapsed_time >= timeout:
                raise Exception("timeout reached, exiting")
            time.sleep(2)

        gpcp = xr.open_dataset(
            target_path, backend_kwargs={"storage_options": fsspec_args}, engine="zarr"
        )

        assert (
            gpcp.title
            == "Global Precipitation Climatatology Project (GPCP) Climate Data Record (CDR), Daily V1.3"
        )
        # --prune prunes to two time steps by default, so we expect 2 items here
        assert len(gpcp.precip) == 2
        print(gpcp)
