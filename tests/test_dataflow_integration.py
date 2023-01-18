import json
import subprocess
import tempfile


def test_dataflow_integration():

    bucket = "gs://pangeo-forge-runner-ci-testing"
    config = {
        "Bake": {
            "prune": True,
            "bakery_class": "pangeo_forge_runner.bakery.dataflow.DataflowBakery",
        },
        "DataflowBakery": {"temp_gcs_location": bucket + "/temp"},
        "TargetStorage": {
            "fsspec_class": "gcsfs.GCSFileSystem",
            "root_path": bucket + "/target/{job_id}",
        },
        "InputCacheStorage": {
            "fsspec_class": "gcsfs.GCSFileSystem",
            "root_path": bucket + "/input-cache/{job_id}",
        },
        "MetadataCacheStorage": {
            "fsspec_class": "gcsfs.GCSFileSystem",
            "root_path": bucket + "/metadata-cache/{job_id}",
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
            "0.9.x",
            "-f",
            f.name,
        ]
        proc = subprocess.run(cmd)  # noqa: F841
