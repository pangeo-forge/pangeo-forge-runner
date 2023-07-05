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


@pytest.fixture
def recipes_version_ref():
    # FIXME: recipes version matrix is currently determined by github workflows matrix
    # in the future, it should be set by pangeo-forge-runner venv feature?
    pip_list = subprocess.check_output("pip list".split()).decode("utf-8").splitlines()
    recipes_version = [
        p.split()[-1] for p in pip_list if p.startswith("pangeo-forge-recipes")
    ][0]
    # the recipes_version is a 3-element semantic version of form `0.A.B` where A is either minor
    # version `9` or `10`. the test feedstock (pforgetest/gpcp-from-gcs-feedstock) has tags for
    # each of these minor versions, of the format `0.A.x`, so we translate the installed version
    # of pangeo-forge-recipes to one of the valid tags (either `0.9.x` or `0.10.x`) here.
    return f"0.{recipes_version.split('.')[1]}.x"
