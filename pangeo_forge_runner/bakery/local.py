"""
Bakery for baking pangeo-forge recipes in Direct Runner
"""
from apache_beam.pipeline import PipelineOptions
from .base import Bakery
from traitlets import Integer


class LocalDirectBakery(Bakery):
    """
    Bake recipes on your local machine, without docker.

    Uses the Apache Beam DirectRunner
    """

    # DirectRunner blocks the pipeline.run() call until run is completed
    blocking = True

    num_workers = Integer(
        0,
        config=True,
        help="""
        Number of workers to use when baking the recipe.

        Defaults to 0, which is interpreted by Apache beam to be the
        number of CPUs on the machine
        """,
    )

    def get_pipeline_options(
        self, job_name: str, container_image: str
    ) -> PipelineOptions:
        """
        Return PipelineOptions for use with this Bakery
        """
        # Set flags explicitly to empty so Apache Beam doesn't try to parse the commandline
        # for pipeline options - we have traitlets doing that for us.
        return PipelineOptions(
            flags=[],
            runner="DirectRunner",
            direct_running_mode="multiprocessing",
            direct_num_workers=self.num_workers,
            save_main_session=True,
            # this might solve serialization issues; cf. https://beam.apache.org/blog/beam-2.36.0/
            pickle_library="cloudpickle",
        )