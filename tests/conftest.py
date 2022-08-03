import signal
import pytest
import subprocess
import tempfile
import os
import secrets

@pytest.fixture(scope='session')
def minio():
    """
    Start a temporary minio instance & return values used to connect to it

    yields a dictionary with the following keys:
    - endpoint - the HTTP endpoint to use to talk to the minio API
    - username - the minio root username (aka 'key' in S3 parlance)
    - password - the minio root password
    """
    username = secrets.token_hex(16)
    password = secrets.token_hex(16)
    address = '127.0.0.1:19555'
    endpoint = f'http://{address}'

    env = os.environ.copy()
    env.update({
        'MINIO_ROOT_USER': username,
        'MINIO_ROOT_PASSWORD': password
    })
    with tempfile.TemporaryDirectory() as d:
        proc = subprocess.Popen([
                'minio',
                'server',
                d,
                '--address', address
            ],
            env=env
        )

        yield {
            'endpoint': endpoint,
            'username': username,
            'password': password
        }

        # Cleanup minio server during teardown
        proc.send_signal(signal.SIGTERM)
        proc.wait()

        assert proc.returncode == 0