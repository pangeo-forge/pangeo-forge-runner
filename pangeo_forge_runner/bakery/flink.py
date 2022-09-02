"""
Bakery for baking pangeo-forge recipes in GCP DataFlow
"""
import hashlib
import json
import shutil
import subprocess
import tempfile

from apache_beam.pipeline import PipelineOptions
from traitlets import Unicode

from .base import Bakery


# Copied from https://github.com/jupyterhub/kubespawner/blob/7d6d82c2be469dd76f770d6f6ed0d1dade6b24a7/kubespawner/utils.py#L8
# But I wrote it there too - is it really stealing if you're stealing from yourself?
def generate_hashed_slug(slug, limit=63, hash_length=6):
    """
    Generate a unique name that's within a certain length limit

    Most k8s objects have a 63 char name limit. We wanna be able to compress
    larger names down to that if required, while still maintaining some
    amount of legibility about what the objects really are.

    If the length of the slug is shorter than the limit - hash_length, we just
    return slug directly. If not, we truncate the slug to (limit - hash_length)
    characters, hash the slug and append hash_length characters from the hash
    to the end of the truncated slug. This ensures that these names are always
    unique no matter what.
    """
    if len(slug) < (limit - hash_length):
        return slug

    slug_hash = hashlib.sha256(slug.encode("utf-8")).hexdigest()

    return "{prefix}-{hash}".format(
        prefix=slug[: limit - hash_length - 1],
        hash=slug_hash[:hash_length],
    ).lower()


class FlinkBakery(Bakery):
    """
    Bake a Pangeo Forge recipe on a Flink cluster
    """

    flink_version = Unicode(
        "1.15",
        config=True,
        description="""
        Version of Flink to use.

        Must be a version supported by the Flink Operator installed in the cluster
        """,
    )

    def make_flink_deployment(self, name: str, worker_image: str):
        """
        Return YAML for a FlinkDeployment
        """
        image = f"flink:{self.flink_version}"
        flink_version_str = f'v{self.flink_version.replace(".", "_")}'
        return {
            "apiVersion": "flink.apache.org/v1beta1",
            "kind": "FlinkDeployment",
            "metadata": {"name": name},
            "spec": {
                "image": image,
                "flinkVersion": flink_version_str,
                "flinkConfiguration": {"taskmanager.numberOfTaskSlots": "2"},
                "serviceAccount": "flink",
                "jobManager": {"resource": {"memory": "2048m", "cpu": 1}},
                "taskManager": {
                    "replicas": 5,
                    "resource": {"memory": "2048m", "cpu": 1},
                    "podTemplate": {
                        "spec": {
                            "containers": [
                                {
                                    "name": "beam-worker-pool",
                                    "image": worker_image,
                                    "ports": [{"containerPort": 50000}],
                                    "readinessProbe": {
                                        "tcpSocket": {"port": 50000},
                                        "initialDelaySeconds": 30,
                                        "periodSeconds": 60,
                                    },
                                    "command": ["/opt/apache/beam/boot"],
                                    "args": ["--worker_pool"],
                                }
                            ]
                        }
                    },
                },
            },
        }

    def get_pipeline_options(
        self, job_name: str, container_image: str
    ) -> PipelineOptions:
        """
        Return PipelineOptions for use with this Bakery
        """

        # We use `kubectl` to talk to kubernetes, so let's make sure it exists
        if shutil.which("kubectl") is None:
            raise ValueError("kubectl is required for FlinkBakery to work")

        # Flink cluster names have a 45 char limit
        cluster_name = generate_hashed_slug(job_name, 45)

        # Create the temp flink cluster
        with tempfile.NamedTemporaryFile(mode="w") as f:
            f.write(
                json.dumps(self.make_flink_deployment(cluster_name, container_image))
            )
            f.flush()
            # FIXME: Add a timeout here?
            cmd = ["kubectl", "apply", "-f", f.name]
            subprocess.check_call(cmd)

        # Wait for the cluster to be ready
        cmd = [
            "kubectl",
            "wait",
            f"flinkdeployments.flink.apache.org/{cluster_name}",
            "--for=jsonpath=.status.jobManagerDeploymentStatus=READY",
        ]
        subprocess.check_call(cmd)

        # port-forward the thing
        cmd = [
            "kubectl",
            "port-forward",
            "--pod-running-timeout=2m0s",
            "--address",
            "127.0.0.1",  # Just ipv4 tx
            f"svc/{cluster_name}-rest",
            # 0 will allocate a random local port
            "0:8081",
        ]
        # Let's read out the line of output from kubectl so we can figure out what port
        # `kubectl` has bound to
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
        line = p.stdout.readline().decode()
        # I could use regex here, but then I'll have two problems (at least)
        # The line provided by kubectl looks like `Forwarding from 127.0.0.1:59408 -> 8081`
        # We just want the randomly generated port there
        listen_address = line.split(" ")[2]
        # Use rsplit in case we listen on ipv6 stuff in the future
        listen_port = listen_address.rsplit(":", 1)[1]
        print(f"You can see the flink dashboard at http://127.0.0.1:{listen_port}")

        # Set flags explicitly to empty so Apache Beam doesn't try to parse the commandline
        # for pipeline options - we have traitlets doing that for us.
        opts = dict(
            flags=[],
            runner="FlinkRunner",
            # FIXME: This should be the deployed flink master URL
            flink_master=f"127.0.0.1:{listen_port}",
            flink_submit_uber_jar=True,
            environment_type="EXTERNAL",
            environment_config="0.0.0.0:50000",
            # https://cloud.google.com/dataflow/docs/resources/faq#how_do_i_handle_nameerrors
            save_main_session=True,
            # this might solve serialization issues; cf. https://beam.apache.org/blog/beam-2.36.0/
            pickle_library="cloudpickle",
        )
        return PipelineOptions(**opts)
