"""
Bakery for baking pangeo-forge recipes in GCP DataFlow
"""
import shutil
import subprocess

from apache_beam.pipeline import PipelineOptions
from traitlets import Bool, Integer, TraitError, Unicode, default, validate

from .base import Bakery


class DataflowBakery(Bakery):
    """
    Bake a Pangeo Forge recipe on GCP Dataflow
    """

    project_id = Unicode(
        None,
        allow_none=True,
        config=True,
        help="""
        GCP Project to submit the Dataflow job into.

        Defaults to the output of `gcloud config get-value project` if unset.
        Must be set for the Bakery to function.
        """,
    )

    @default("project_id")
    def _default_project_id(self):
        """
        Set default project_id from `gcloud` if it is set
        """
        if not shutil.which("gcloud"):
            # If `gcloud` is not installed, just do nothing
            return None
        return subprocess.check_output(
            ["gcloud", "config", "get-value", "project"], encoding="utf-8"
        ).strip()

    region = Unicode(
        "us-central1",
        config=True,
        help="""
        GCP Region to submit the Dataflow jobs into
        """,
    )

    machine_type = Unicode(
        "n1-highmem-2",
        config=True,
        help="""
        GCP Machine type to use for the Dataflow jobs.

        Ignored if use_dataflow_prime is set.
        """,
    )

    use_dataflow_prime = Bool(
        False,
        config=True,
        help="""
        Use GCP's DataFlow Prime instead of regular DataFlow.

        https://cloud.google.com/dataflow/docs/guides/enable-dataflow-prime has more information
        on the advantages of dataflow prime.
        """,
    )

    use_public_ips = Bool(
        False,
        config=True,
        help="""
        Use public IPs for the Dataflow workers.

        Set to false for projects that have policies against VM
        instances having their own public IPs
        """,
    )

    temp_gcs_location = Unicode(
        None,
        allow_none=True,
        config=True,
        help="""
        GCS URL under which to put temporary files required to launch dataflow jobs

        *Must* be set, and be a gs:// URL.
        """,
    )

    max_num_workers = Integer(
        None,
        allow_none=True,
        config=True,
        help="""
        Maximum number of workers this job can be autoscaled to.

        Set to None (default) for no limit.
        """,
    )

    service_account_email = Unicode(
        None,
        allow_none=True,
        config=True,
        help="""
        If using a GCP service account to deploy Dataflow jobs, this option specifies the
        service account email address, which must be set to avoid permissions issues during
        pipeline execution. If you are using GCP user creds, do not set this value.

        Defaults to the output of `gcloud config get-value account` if this value is a
        service account email address. If this value is a user email address, defaults
        to `None`.
        """,
    )

    @default("service_account_email")
    def _default_service_account_email(self):
        """
        Set default service_account_email from `gcloud` if it is set
        """
        if not shutil.which("gcloud"):
            # If `gcloud` is not installed, just do nothing
            return None
        current_account = subprocess.check_output(
            ["gcloud", "config", "get-value", "account"], encoding="utf-8"
        ).strip()
        return (
            # If logged into `gcloud` with a user account, setting this option will result in an
            # error such as `Current user cannot act as service account` when the Dataflow job is
            # deployed. So only set a default value here if `gcloud` account is a service account.
            current_account
            if current_account.endswith(".iam.gserviceaccount.com")
            else None
        )

    @validate("temp_gcs_location")
    def _validate_temp_gcs_location(self, proposal):
        """
        Ensure that temp_gcs_location is a gs:// URL
        """
        if not proposal["value"].startswith("gs://"):
            raise TraitError("DataflowBakery.temp_gcs_location must be a gs:// URL")
        return proposal["value"]

    def get_pipeline_options(
        self, job_name: str, container_image: str, extra_options: dict
    ) -> PipelineOptions:
        """
        Return PipelineOptions for use with this Bakery
        """
        if self.temp_gcs_location is None:
            raise ValueError("DataflowBakery.temp_bucket must be set")
        if self.project_id is None:
            raise ValueError("DataflowBakery.project_id must be set")

        if self.use_dataflow_prime:
            # dataflow prime does not support setting machine types explicitly!
            sizing_options = {"dataflow_service_options": ["enable_prime"]}
        else:
            sizing_options = {"machine_type": self.machine_type}

        # Set flags explicitly to empty so Apache Beam doesn't try to parse the commandline
        # for pipeline options - we have traitlets doing that for us.
        opts = dict(
            flags=[],
            runner="DataflowRunner",
            project=self.project_id,
            job_name=job_name,
            max_num_workers=self.max_num_workers,
            temp_location=self.temp_gcs_location,
            use_public_ips=self.use_public_ips,
            region=self.region,
            # The v2 Runner is required to use custom container images
            # https://cloud.google.com/dataflow/docs/guides/using-custom-containers#usage
            experiments=["use_runner_v2"],
            sdk_container_image=container_image,
            sdk_location="container",
            # https://cloud.google.com/dataflow/docs/resources/faq#how_do_i_handle_nameerrors
            save_main_session=True,
            # this might solve serialization issues; cf. https://beam.apache.org/blog/beam-2.36.0/
            pickle_library="cloudpickle",
            **(sizing_options | extra_options)
        )
        if self.service_account_email:
            opts.update({"service_account_email": self.service_account_email})
        return PipelineOptions(**opts)
