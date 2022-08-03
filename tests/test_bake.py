import subprocess
import tempfile
import json


def test_gpcp_bake(minio):
    fsspec_args = {
        "key": minio['username'],
        "secret": minio['password'],
        "client_kwargs": {
            "endpoint_url": minio["endpoint"]
        }
    }

    config = {
        'Bake': {
            'prune': True,
            'bakery_class': 'pangeo_forge_runner.bakery.local.LocalDirectBakery'
        },
        'TargetStorage': {
            'fsspec_class': 's3fs.S3FileSystem',
            'fsspec_args': fsspec_args,
            'root_path': 's3://gpcp/target/{job_id}'
        },
        'InputCacheStorage': {
            'fsspec_class': 's3fs.S3FileSystem',
            'fsspec_args': fsspec_args,
            'root_path': 's3://gpcp/input-cache/{job_id}'
        },
        'MetadataCacheStorage': {
            'fsspec_class': 's3fs.S3FileSystem',
            'fsspec_args': fsspec_args,
            'root_path': 's3://gpcp/metadata-cache/{job_id}'
        }
    }

    with tempfile.NamedTemporaryFile('w', suffix='.json') as f:
        json.dump(config, f)
        f.flush()
        cmd = [
            'pangeo-forge-runner',
            'bake',
            '--repo',
            'https://github.com/pangeo-forge/gpcp-feedstock.git',
            '--ref',
            '2cde04745189665a1f5a05c9eae2a98578de8b7f',
            '-f',
            f.name
        ]
        print(f.name)
        proc = subprocess.run(cmd)

        assert proc.returncode == 0

        # `mc` isn't the best way to do this, but it'll do for now
        with tempfile.TemporaryDirectory() as mcd:
            cmd = [
                'mc',
                '--config-dir', mcd,
                'alias', 'set', 'local',
                minio['endpoint'], minio['username'], minio['password']
            ]

            subprocess.run(cmd, check=True)

            cmd = [
                'mc',
                '--config-dir', mcd,
                'ls', '--recursive',
                'local'
            ]
            subprocess.run(cmd, check=True)