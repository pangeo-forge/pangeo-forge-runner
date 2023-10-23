import json
import subprocess
import tempfile
import time

import xarray as xr


def test_flink_bake(minio):
    fsspec_args = {
        "key": minio["username"],
        "secret": minio["password"],
        "client_kwargs": {"endpoint_url": minio["endpoint"]},
    }

    config = {
        "Bake": {
            "prune": True,
            "bakery_class": "pangeo_forge_runner.bakery.flink.FlinkOperatorBakery",
            # there must be a job-server jar available for the matching
            # `apache-beam` and `FlinkOperatorBakery.flink_version` here:
            # https://repo.maven.apache.org/maven2/org/apache/beam/beam-runners-flink-1.16-job-server/
            "container_image": "apache/beam_python3.9_sdk:2.47.0",
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
        "FlinkOperatorBakery": {
            "flink_version": "1.16",
            "job_manager_resources": '{"memory": "1024m", "cpu": 1.0}',
            "task_manager_resources": '{"memory": "2048m", "cpu": 1.0}',
            "parallelism": "1",
            "flink_configuration": '{"taskmanager.numberOfTaskSlots": "1", "taskmanager.memory.jvm-overhead.max": "2048m"}',
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
            "main",
            "-f",
            f.name,
        ]
        proc = subprocess.run(cmd, capture_output=True)

        assert proc.returncode == 0

        # We should have some kinda 'has this completed?' check here
        # Instead, I just wait for 3min
        time.sleep(60 * 3)
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
