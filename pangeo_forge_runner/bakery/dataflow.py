"""
Bakery for baking pangeo-forge recipes in GCP DataFlow
"""
import shutil
from apache_beam.pipeline import PipelineOptions
from .base import Bakery
from traitlets import Unicode, Bool, default, validate, TraitError
import subprocess


class DataflowBakery(Bakery):
    project_id = Unicode(
        None,
        allow_none=True,
        config=True,
        help="""
        GCP Project to submit the Dataflow job into.

        Defaults to the output of `gcloud config get-value project` if unset.
        """
    )

    @default("project_id")
    def _default_project_id(self):
        """
        Set default project_id from `gcloud` if it is set
        """
        if not shutil.which('gcloud'):
            # If `gcloud` is not installed, just do nothing
            return None
        return subprocess.check_output(
            ["gcloud", "config", "get-value", "project"],
            encoding='utf-8'
        ).strip()

    region = Unicode(
        "us-central1",
        config=True,
        help="""
        GCP Region to submit the Dataflow jobs into
        """
    )

    machine_type = Unicode(
        "n1-highmem-2",
        config=True,
        help="""
        GCP Machine type to use for the Dataflow jobs
        """
    )

    use_public_ips = Bool(
        False,
        config=True,
        help="""
        Use public IPs for the Dataflow workers.

        Set to false for projects that have policies against VM
        instances having their own public IPs
        """
    )

    temp_gcs_location = Unicode(
        None,
        allow_none=True,
        config=True,
        help="""
        GCS URL under which to put temporary files required to launch dataflow jobs

        *Must* be set, and be a gs:// URL.
        """
    )

    @validate('temp_gcs_location')
    def _validate_temp_gcs_location(self, proposal):
        """
        Ensure that temp_gcs_location is a gs:// URL
        """
        if not proposal['value'].startswith('gs://'):
            raise TraitError('DataflowBakery.temp_gcs_location must be a gs:// URL')
        return proposal['value']


    def get_pipeline_options(self, job_name: str, container_image: str) -> PipelineOptions:
        """
        Return PipelineOptions for use with this Bakery
        """
        if self.temp_gcs_location is None:
            raise ValueError('DataflowBakery.temp_bucket must be set')
        if self.project_id is None:
            raise ValueError('DataflowBakery.project_id must be set')

        # Set flags explicitly to empty so Apache Beam doesn't try to parse the commandline
        # for pipeline options - we have traitlets doing that for us.
        return PipelineOptions(
            flags=[],
            runner="DataflowRunner",
            project=self.project_id,
            job_name=job_name,
            temp_location=self.temp_gcs_location,
            use_public_ips=self.use_public_ips,
            region=self.region,
            # https://cloud.google.com/dataflow/docs/guides/using-custom-containers#usage
            experiments=["use_runner_v2"],
            sdk_container_image=container_image,
            # https://cloud.google.com/dataflow/docs/resources/faq#how_do_i_handle_nameerrors
            save_main_session=True,
            # this might solve serialization issues; cf. https://beam.apache.org/blog/beam-2.36.0/
            pickle_library="cloudpickle",
            machine_type=self.machine_type
        )

