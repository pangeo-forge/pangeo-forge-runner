"""
Command to run a pangeo-forge recipe
"""
from apache_beam import Pipeline
from datetime import datetime
from .base import BaseCommand, common_aliases, common_flags
from pathlib import Path
import tempfile
from .. import Feedstock
from ..stream_capture import redirect_stderr, redirect_stdout
from traitlets import Bool, Type
from ..runners.dataflow import DataflowRunner
from pangeo_forge_recipes.storage import StorageConfig

from ..storage import TargetStorage, InputCacheStorage, MetadataCacheStorage


class Bake(BaseCommand):
    """
    Application to bake a pangeo forge recipe in a given runner
    """
    aliases = common_aliases
    flags = common_flags | {
        'prune': (
            {'Bake': {'prune': True}},
            'Prune the recipe to run only for 2 time steps'
        )
    }

    prune = Bool(
        False,
        config=True,
        help="""
        Prune the recipe to only run for 2 time steps.

        Makes it much easier to test recipes!
        """
    )

    runner_class = Type(
        default_value=DataflowRunner,
        entry_point_group="pangeo_forge_runner.runners",
        config=True,
        help="""
        The class to use for configuring the runner when baking.
        """,
    )

    def start(self):
        with tempfile.TemporaryDirectory() as d:
            self.fetch(d)
            feedstock = Feedstock(Path(d))
            self.log.info("Parsing recipes", extra={'status': 'running'})
            with redirect_stderr(self.log, {'status': 'running'}), redirect_stdout(self.log, {'status': 'running'}):
                recipes = feedstock.parse_recipes()

            if self.prune:
                # Prune all recipes if we're asked to
                recipes = {k: r.copy_pruned() for k, r in recipes.items()}

            runner = self.runner_class(
                parent=self
            )


            target_storage = TargetStorage(parent=self)
            input_cache_storage = InputCacheStorage(parent=self)
            metadata_cache_storage = MetadataCacheStorage(parent=self)

            for name, recipe in recipes.items():
                job_name=f'{name}-{recipe.sha256().hex()}-{int(datetime.now().timestamp())}'
                recipe.storage_config = StorageConfig(
                    target_storage.get_forge_target(job_id=job_name),
                    input_cache_storage.get_forge_target(job_id=job_name),
                    metadata_cache_storage.get_forge_target(job_id=job_name)
                )
                pipeline_options = runner.get_pipeline_options(
                    # FIXME: Put in repo / ref here
                    job_name=job_name,
                    # FIXME: Bring this in from meta
                    container_image='pangeo/forge:8a862dc'
                )
                pipeline = Pipeline(options=pipeline_options)
                # Chain our recipe to the pipeline
                pipeline | recipe.to_beam()
                result = pipeline.run()
                job_id = result.job_id()
                self.log.info(
                    f"Submitted job {job_id} for recipe {name}",
                    extra={
                        'job_id': job_id,
                        'recipe': name,
                        'status': 'submitted'
                    }
                )



