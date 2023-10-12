"""
Bakery for baking pangeo-forge recipes in GCP DataFlow
"""
import hashlib
import json
import shutil
import subprocess
import tempfile
import time

import escapism
from apache_beam.pipeline import PipelineOptions
from traitlets import Dict, Integer, Unicode

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


class FlinkOperatorBakery(Bakery):
    """
    Bake a Pangeo Forge recipe on a Flink cluster based on the Apache Flink k8s Operator

    Requires a kubernetes cluster with https://github.com/apache/flink-kubernetes-operator
    installed
    """

    # Not actually, but we don't have a job_id to return.
    # that looks like just a dataflow concept, we'll have to refactor
    blocking = True

    flink_version = Unicode(
        "1.15",
        config=True,
        help="""
        Version of Flink to use.

        Must be a version supported by the Flink Operator installed in the cluster
        """,
    )

    flink_configuration = Dict(
        {"taskmanager.numberOfTaskSlots": "2"},
        config=True,
        help="""
        Properties to set as Flink configuration.

        See https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/config/
        for full list of configuration options. Make sure you are looking at the right
        setup for the version of Flink you are using.
        """,
    )

    job_manager_resources = Dict(
        {"memory": "1024m", "cpu": 0.2},
        config=True,
        help="""
        Memory & CPU resources to give to the jobManager pod.

        Passed through to .spec.jobManager.resource in the FlinkDeployment CRD.

        See https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/custom-resource/reference/#resource
        for accepted keys and what they mean. Specifically, note that this is *not*
        specified the same way as kubernetes resource requests in general.

        Note that at least memory *must* be set.
        """,
    )

    task_manager_resources = Dict(
        {"memory": "1024m", "cpu": 0.2},
        config=True,
        help="""
        Memory & CPU resources to give to the taskManager container.

        Passed through to .spec.taskManager.resource in the FlinkDeployment CRD.

        Note this is just the resources for the *taskManager* container only - not
        for the beam executor container where our python code is actually executed.
        That is managed via beam_executor_resources.

        See https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/custom-resource/reference/#resource
        for accepted keys and what they mean. Specifically, note that this is *not*
        specified the same way as kubernetes resource requests in general.

        Note that at least memory *must* be set.
        """,
    )

    beam_executor_resources = Dict(
        {},
        config=True,
        help="""
        Resources to be given the beam executor container.

        Passed through to the kubernetes specification for the container that
        actually runs the custom python code we have. See
        https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#resource-requests-and-limits-of-pod-and-container
        for possible options.

        Note that this is *not* specified the same way as other resource
        request config on this class.
        """,
    )

    parallelism = Integer(
        None,
        allow_none=True,
        config=True,
        help="""
        The degree of parallelism to be used when distributing operations onto workers.
        If the parallelism is not set, the configured Flink default is used,
        or 1 if none can be found.
        """,
    )

    max_parallelism = Integer(
        None,
        allow_none=True,
        config=True,
        help="""
        The pipeline wide maximum degree of parallelism to be used.
        The maximum parallelism specifies the upper limit for dynamic scaling
        and the number of key groups used for partitioned state.
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
                "flinkConfiguration": self.flink_configuration,
                "serviceAccount": "flink",
                "jobManager": {"resource": self.job_manager_resources},
                "taskManager": {
                    "replicas": 5,
                    "resource": self.task_manager_resources,
                    "podTemplate": {
                        "spec": {
                            "containers": [
                                {
                                    "name": "beam-worker-pool",
                                    "image": worker_image,
                                    "ports": [{"containerPort": 50000}],
                                    "readinessProbe": {
                                        # Don't mark this container as ready until the beam SDK harnass starts
                                        "tcpSocket": {"port": 50000},
                                        "periodSeconds": 10,
                                    },
                                    "resources": self.beam_executor_resources,
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
        self, job_name: str, container_image: str, extra_options: dict
    ) -> PipelineOptions:
        """
        Return PipelineOptions for use with this Bakery
        """

        # We use `kubectl` to talk to kubernetes, so let's make sure it exists
        if shutil.which("kubectl") is None:
            raise ValueError("kubectl is required for FlinkBakery to work")

        # Flink cluster names have a 45 char limit, and can only contain - special char
        # And no uppercase characters are allowed
        cluster_name = generate_hashed_slug(
            escapism.escape(job_name, escape_char="-").lower(), 45
        )

        # Create the temp flink cluster
        with tempfile.NamedTemporaryFile(mode="w") as f:
            f.write(
                json.dumps(self.make_flink_deployment(cluster_name, container_image))
            )
            f.flush()
            # FIXME: Add a timeout here?
            # --wait is needed even though we have a kubectl wait below.
            # Without it, kubectl wait tries to read the CRD before it has *any* status
            # fields and fails miserably
            cmd = ["kubectl", "apply", "--wait", "-f", f.name]
            subprocess.check_call(cmd)

        time.sleep(5)
        # Wait for the cluster to be ready
        cmd = [
            "kubectl",
            "wait",
            f"flinkdeployments.flink.apache.org/{cluster_name}",
            "--for=jsonpath=.status.jobManagerDeploymentStatus=READY",
            # FIXME: Parameterize this wait time
            "--timeout=5m",
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

        print(f"You can run '{' '.join(cmd)}' to make the Flink Dashboard available!")

        for k, v in dict(
            parallelism=self.parallelism,
            max_parallelism=self.max_parallelism,
        ).items():
            if v:  # if None, don't pass these options to Flink
                extra_options |= {k: v}

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
            **extra_options,
        )
        return PipelineOptions(**opts)
