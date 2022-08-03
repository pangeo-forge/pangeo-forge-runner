from traitlets import Bool
from traitlets.config import LoggingConfigurable
from apache_beam.pipeline import PipelineOptions

class Bakery(LoggingConfigurable):
    """
    Base class for Bakeries where recipes can be run.

    This provides an opinionated and consistent wrapper to an
    Apache Beam runner.
    """

    blocking = Bool(
        False,
        config=False,
        help="""
        Set to True if this Bakery will default block calls to pipeline.run()
        """
    )

    def get_pipeline_options(
        self, job_name: str, container_image: str
    ) -> PipelineOptions:
        """
        Return a PipelineOptions object that will tell a pipeline to use this Bakery

        job_name: A unique string representing this particular run
        container_image: A docker image spec that should be used in this run,
                         if the Bakery supports using docker images

        """
        raise NotImplementedError('Override get_pipeline_options in subclass')
