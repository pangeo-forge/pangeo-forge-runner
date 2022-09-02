import os
import secrets
import signal
import socket
import subprocess
import tempfile

import pytest


# Stolen from https://stackoverflow.com/a/28950776
@pytest.fixture(scope="session")
def local_ip():
    """
    Return IP of current machine

    Hopefully, this is resolveable by both code running on the machine
    as well as whatever kubernetes cluster is being used to run tests.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(0)
    try:
        # doesn't even have to be reachable
        s.connect(("10.254.254.254", 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = "127.0.0.1"
    finally:
        s.close()
    return IP


@pytest.fixture(scope="session")
def minio(local_ip):
    """
    Start a temporary minio instance & return values used to connect to it

    yields a dictionary with the following keys:
    - endpoint - the HTTP endpoint to use to talk to the minio API
    - username - the minio root username (aka 'key' in S3 parlance)
    - password - the minio root password
    """
    username = secrets.token_hex(16)
    password = secrets.token_hex(16)
    address = f"{local_ip}:19555"
    endpoint = f"http://{address}"

    env = os.environ.copy()
    env.update({"MINIO_ROOT_USER": username, "MINIO_ROOT_PASSWORD": password})
    with tempfile.TemporaryDirectory() as d:
        proc = subprocess.Popen(["minio", "server", d, "--address", address], env=env)

        yield {"endpoint": endpoint, "username": username, "password": password}

        # Cleanup minio server during teardown
        proc.send_signal(signal.SIGTERM)
        proc.wait()

        assert proc.returncode == 0
