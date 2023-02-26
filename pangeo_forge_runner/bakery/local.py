"""
Bakery for baking pangeo-forge recipes in Direct Runner
"""
from apache_beam.pipeline import PipelineOptions
from traitlets import Integer, Unicode

from .base import Bakery


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

    direct_running_mode = Unicode(
        "multi_threading",
        config=True,
        help="""
        One of 'in_memory', 'multi_threading', 'multi_processing'.

        in_memory: Runner and workers’ communication happens in memory (not through gRPC). This is a default mode.
        multi_threading: Runner and workers communicate through gRPC and each worker runs in a thread.
        multi_processing: Runner and workers communicate through gRPC and each worker runs in a subprocess.

        multi_processing is closest to most production runners, as it enables real usage of multiple
        CPUs on the host machine. **However**, it can mess up logging, so is not the default here.

        https://beam.apache.org/documentation/runners/direct/#setting-parallelism has more
        information.
        """,
    )

    def get_pipeline_options(
        self, job_name: str, container_image: str, extra_options: dict
    ) -> PipelineOptions:
        """
        Return PipelineOptions for use with this Bakery
        """
        # Set flags explicitly to empty so Apache Beam doesn't try to parse the commandline
        # for pipeline options - we have traitlets doing that for us.
        return PipelineOptions(
            flags=[],
            runner="DirectRunner",
            direct_running_mode=self.direct_running_mode,
            direct_num_workers=self.num_workers,
            save_main_session=True,
            # this might solve serialization issues; cf. https://beam.apache.org/blog/beam-2.36.0/
            pickle_library="cloudpickle",
            **extra_options
        )
