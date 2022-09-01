"""
Bakery for baking pangeo-forge recipes in GCP DataFlow
"""
from apache_beam.pipeline import PipelineOptions
from traitlets import Bool, TraitError, Unicode, default, validate

from .base import Bakery


class FlinkBakery(Bakery):
    """
    Bake a Pangeo Forge recipe on a Flink cluster
    """

    def get_pipeline_options(
        self, job_name: str, container_image: str
    ) -> PipelineOptions:
        """
        Return PipelineOptions for use with this Bakery
        """

        # Set flags explicitly to empty so Apache Beam doesn't try to parse the commandline
        # for pipeline options - we have traitlets doing that for us.
        opts = dict(
            flags=[],
            runner="FlinkRunner",
            # FIXME: This should be the deployed flink master URL
            flink_master='127.0.0.1:8081',
            flink_submit_uber_jar=True,
            environment_type='EXTERNAL',
            environment_config='0.0.0.0:50000',
            # https://cloud.google.com/dataflow/docs/resources/faq#how_do_i_handle_nameerrors
            save_main_session=True,
            # this might solve serialization issues; cf. https://beam.apache.org/blog/beam-2.36.0/
            pickle_library="cloudpickle",
        )
        return PipelineOptions(**opts)
