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
from ..bakery.base import Bakery
from ..bakery.local import LocalDirectBakery
from pangeo_forge_recipes.storage import StorageConfig

from ..storage import TargetStorage, InputCacheStorage, MetadataCacheStorage


class Bake(BaseCommand):
    """
    Application to bake a pangeo forge recipe in a given bakery
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

    bakery_class = Type(
        default_value=LocalDirectBakery,
        klass=Bakery,
        config=True,
        help="""
        The bakery to use when baking
        """,
    )

    def start(self):

        target_storage = TargetStorage(parent=self)
        input_cache_storage = InputCacheStorage(parent=self)
        metadata_cache_storage = MetadataCacheStorage(parent=self)

        self.log.info(f'Target Storage is {target_storage}\n', extra={'status': 'setup'})
        self.log.info(f'Input Cache Storage is {input_cache_storage}\n', extra={'status': 'setup'})
        self.log.info(f'Metadata Cache Storage is {metadata_cache_storage}\n', extra={'status': 'setup'})

        with tempfile.TemporaryDirectory() as d:
            self.fetch(d)
            feedstock = Feedstock(Path(d))

            self.log.info("Parsing recipes...", extra={'status': 'running'})
            with redirect_stderr(self.log, {'status': 'running'}), redirect_stdout(self.log, {'status': 'running'}):
                recipes = feedstock.parse_recipes()

            if self.prune:
                # Prune all recipes if we're asked to
                recipes = {k: r.copy_pruned() for k, r in recipes.items()}

            bakery: Bakery = self.bakery_class(
                parent=self
            )



            for name, recipe in recipes.items():
                job_name=f'{name}-{recipe.sha256().hex()}-{int(datetime.now().timestamp())}'

                recipe.storage_config = StorageConfig(
                    target_storage.get_forge_target(job_name=job_name),
                    input_cache_storage.get_forge_target(job_name=job_name),
                    metadata_cache_storage.get_forge_target(job_name=job_name)
                )

                pipeline_options = bakery.get_pipeline_options(
                    # FIXME: Put in repo / ref here
                    job_name=job_name,
                    # FIXME: Bring this in from meta
                    container_image='pangeo/forge:8a862dc'
                )
                # Set argv explicitly to empty so Apache Beam doesn't try to parse the commandline
                # for pipeline options - we have traitlets doing that for us.
                pipeline = Pipeline(options=pipeline_options, argv=[])
                # Chain our recipe to the pipeline
                pipeline | recipe.to_beam()
                if bakery.blocking:
                    self.log.info(f"Running job for recipe {name}\n",
                        extra={
                            'recipe': 'name',
                            'status': 'running'
                        }
                    )
                    pipeline.run()
                else:
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





