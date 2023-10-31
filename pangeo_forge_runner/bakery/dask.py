"""
Bakery for baking pangeo-forge recipes with DaskRunner
"""
from apache_beam.pipeline import PipelineOptions
from traitlets import Dict, Unicode

from .base import Bakery


class DaskBakery(Bakery):
    """
    Bake recipes on any Dask cluster.

    Uses the Apache Beam DaskRunner.
    """

    client_address = Unicode(
        None,
        allow_none=True,
        config=True,
        help="""
        Address of the Dask client.
        If none, a ``LocalCluster`` is created just for this run.
        """,
    )

    client_kwargs = Dict(
        {},
        config=True,
        help="""
        """,
    )

    local_cluster_kwargs = Dict(
        {},
        config=True,
        help="""
        """,
    )

    def get_pipeline_options(
        self, job_name: str, container_image: str, extra_options: dict
    ) -> PipelineOptions:
        """
        Return PipelineOptions for use with this Bakery
        """
        try:
            # TODO: string registration of "DaskRunner" doesn't appear to work?
            from apache_beam.runners.dask.dask_runner import DaskRunner
        except ImportError as e:
            raise ValueError(
                "Unable to import ``apache_beam.runners.dask.dask_runner.DaskRunner``, "
                "please upgrade to apache-beam>=2.43.0."
            ) from e

        client = None
        if not self.client_address:
            self.log.info("...")
            import dask.distributed as distributed

            cluster = distributed.LocalCluster(**self.local_cluster_kwargs)
            self.client_kwargs |= {"cluster": cluster}
            client = distributed.Client(**self.client_kwargs)
            self.client_address = client.scheduler.address

        if client:
            self.log.info(f"Dask dashboard is available at: {client.dashboard_link}")
        else:
            # TODO: can we get a dashboard link deterministically from an
            # explicitly-passed client_address? If so, let's log it here
            pass

        # Set flags explicitly to empty so Apache Beam doesn't try to parse the commandline
        # for pipeline options - we have traitlets doing that for us.
        return PipelineOptions(
            flags=[],
            runner=DaskRunner(),
            dask_client_address=self.client_address,
            save_main_session=True,
            pickle_library="cloudpickle",
            **extra_options,
        )
