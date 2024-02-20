from typing import List

from apache_beam.pipeline import Pipeline, PipelineOptions
from traitlets import Bool, TraitError
from traitlets.config import LoggingConfigurable

from ..commands.bake import Bake, ExecutionMetadata


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

    def bake(self, pipeline: Pipeline, meta: ExecutionMetadata) -> None:
        """
        Executes the given pipeline using the provided for logs as appropriate.

        pipeline (Pipeline): The pipeline object to be executed.
        meta (ExecutionMetadata): An instance of BakeMetadata containing metadata about the bake process.
        """
        result = pipeline.run()
        job_id = result.job_id()
        self.log.info(
            f"Submitted job {meta.job_id} for recipe {meta.name}",
            extra=meta.to_dict() | {"job_id": job_id, "status": "submitted"},
        )

    @classmethod
    def validate_bake_command(cls, bake_command: Bake) -> List[TraitError]:
        """
        Validates the given bake_command and collects any validation errors.

        This method checks the bake_command against a set of predefined validation
        rules specific to the implementing class. Each rule violation results in
        a TraitError that describes the nature of the violation. If no violations
        are found, an empty list is returned.

        Parameters:
        - bake_command (Bake): The Bake command instance to be validated. It should
        contain all the necessary information and parameters that the validation
        rules will check against.

        Returns:
        List[TraitError]: A list of TraitError objects, each representing a specific
        validation failure. If the bake_command passes all validations, the list will
        be empty.

        Note:
        This method is designed to collect and return all validation errors rather than
        stopping at the first error encountered. This allows for a comprehensive
        overview of all issues within the bake_command at once.
        """
        return []
