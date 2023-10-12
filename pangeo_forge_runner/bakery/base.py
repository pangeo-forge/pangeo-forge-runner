from apache_beam.pipeline import PipelineOptions
from traitlets import Bool
from traitlets.config import LoggingConfigurable


class Bakery(LoggingConfigurable):
    """
    Base class for Bakeries where recipes can be run.

    A Bakery provides an opinionated and consistent wrapper to an
    Apache Beam runner. Users only configure what is important to them,
    and the Bakery takes care of the rest.
    """

    blocking = Bool(
        False,
        config=False,
        help="""
        Set to True if this Bakery will default block calls to pipeline.run()

        Not configurable, should be overriden only by subclasses.
        """,
    )

    def get_pipeline_options(
        self, job_name: str, container_image: str, extra_options: dict
    ) -> PipelineOptions:
        """
        Return a PipelineOptions object that will configure a Pipeline to run on this Bakery

        job_name: A unique string representing this particular run
        container_image: A docker image spec that should be used in this run,
                         if the Bakery supports using docker images
        extra_options: Dictionary of extra options that should be passed on to PipelineOptions

        """
        raise NotImplementedError("Override get_pipeline_options in subclass")
