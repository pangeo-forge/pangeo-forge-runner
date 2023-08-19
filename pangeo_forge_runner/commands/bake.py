"""
Command to run a pangeo-forge recipe
"""
import os
import re
import string
import time
from pathlib import Path

import escapism
from apache_beam import Pipeline, PTransform
from traitlets import Bool, Type, Unicode, validate

from .. import Feedstock
from ..bakery.base import Bakery
from ..bakery.local import LocalDirectBakery
from ..storage import InputCacheStorage, MetadataCacheStorage, TargetStorage
from ..stream_capture import redirect_stderr, redirect_stdout
from .base import BaseCommand, common_aliases, common_flags


class Bake(BaseCommand):
    """
    Command to bake a pangeo forge recipe in a given bakery
    """

    aliases = common_aliases
    flags = common_flags | {
        "prune": (
            {"Bake": {"prune": True}},
            "Prune the recipe to run only for 2 time steps",
        )
    }

    prune = Bool(
        False,
        config=True,
        help="""
        Prune the recipe to only run for 2 time steps.

        Makes it much easier to test recipes!
        """,
    )

    bakery_class = Type(
        default_value=LocalDirectBakery,
        klass=Bakery,
        config=True,
        help="""
        The Bakery to bake this recipe in.

        The Bakery (and its configuration) determine which Apache Beam
        Runner is used, and how options for it are specified.
        Defaults to LocalDirectBakery, which bakes the recipe using Apache
        Beam's "DirectRunner". It doesn't use Docker or the cloud, and runs
        everything locally. Useful only for testing!
        """,
    )

    recipe_id = Unicode(
        None,
        allow_none=True,
        config=True,
        help="""
        Optionally pass this value to run only this recipe_id from the feedstock.

        If empty, all recipes from the feedstock will be run.
        """,
    )

    job_name = Unicode(
        None,
        allow_none=True,
        config=True,
        help="""
        Optionally pass a custom job name for the job run.

        If `None` (the default), a unique name will be generated for the job.
        """,
    )

    @validate("job_name")
    def _validate_job_name(self, proposal):
        """
        Validate that job_name conforms to ^[a-z][-_0-9a-z]{0,62}$ regex.

        That's what is valid in dataflow job names, so let's keep everyone
        in that range.

        Dataflow job names adhere to the following GCP cloud labels requirements:
        https://cloud.google.com/resource-manager/docs/creating-managing-labels#requirements
        """
        validating_regex = r"^[a-z][-_0-9a-z]{0,62}$"
        if not re.match(validating_regex, proposal.value):
            raise ValueError(
                f"job_name must match the regex {validating_regex}, instead found {proposal.value}"
            )
        return proposal.value

    container_image = Unicode(
        # Provides apache_beam 2.42, which we pin to in setup.py
        "quay.io/pangeo/forge:5e51a29",
        config=True,
        help="""
        Container image to use for this job.

        Should be accessible to whatever Beam runner is being used.

        Note that some runners (like the local one) may not support this!
        """,
    )

    def autogenerate_job_name(self):
        """
        Autogenerate a readable job_name
        """
        # special case local checkouts, as no contentprovider is used
        safe_chars = string.ascii_lowercase + string.digits
        if os.path.exists(self.repo):
            name = "local-"
            name += escapism.escape(
                os.path.basename(os.path.abspath(self.repo)),
                safe=safe_chars,
                escape_char="-",
            )
            if self.feedstock_subdir != "feedstock":
                name += "-" + escapism.escape(
                    self.feedstock_subdir, safe=safe_chars, escape_char="-"
                )
            return name.lower()

        # special-case github because it is so common
        if self.repo.startswith("https://github.com/"):
            _, user, repo = self.repo.rsplit("/", 2)
            # Get rid of the '.git' at the end, if it exists
            if repo.endswith(".git"):
                repo = repo[:-4]
            job_name = f"gh-{user}-{repo}-{self.picked_content_provider.content_id}"
        else:
            # everything other than github
            job_name = self.repo
            if self.picked_content_provider.content_id is not None:
                job_name += self.picked_content_provider.content_id

        # Always append current ts to job name, to make it unique
        job_name += "-" + str(int(time.time()))

        return job_name

    def start(self):
        """
        Start the baking process
        """
        # Create our storage configurations. Traitlets will do its magic, populate these
        # with appropriate config from config file / commandline / defaults.
        target_storage = TargetStorage(parent=self)
        input_cache_storage = InputCacheStorage(parent=self)
        metadata_cache_storage = MetadataCacheStorage(parent=self)

        self.log.info(
            f"Target Storage is {target_storage}\n", extra={"status": "setup"}
        )
        self.log.info(
            f"Input Cache Storage is {input_cache_storage}\n", extra={"status": "setup"}
        )
        self.log.info(
            f"Metadata Cache Storage is {metadata_cache_storage}\n",
            extra={"status": "setup"},
        )

        with self.fetch() as checkout_dir:
            if not self.job_name:
                self.job_name = self.autogenerate_job_name()

            callable_args_injections = {
                "StoreToZarr": {
                    "target_root": target_storage.get_forge_target(
                        job_name=self.job_name
                    ),
                }
            }

            cache_target = input_cache_storage.get_forge_target(job_name=self.job_name)
            if cache_target:
                callable_args_injections |= {
                    # FIXME: a plugin/entrypoint system should handle injections.
                    # hardcoding object names here assumes too much.
                    "OpenURLWithFSSpec": {"cache": cache_target},
                }

            feedstock = Feedstock(
                Path(checkout_dir) / self.feedstock_subdir,
                prune=self.prune,
                callable_args_injections=callable_args_injections,
            )

            self.log.info("Parsing recipes...", extra={"status": "running"})
            with redirect_stderr(self.log, {"status": "running"}), redirect_stdout(
                self.log, {"status": "running"}
            ):
                recipes = feedstock.parse_recipes()

            if self.recipe_id:
                if self.recipe_id not in recipes:
                    raise ValueError(f"{self.recipe_id=} not in {list(recipes)}")
                self.log.info(f"Baking only recipe_id='{self.recipe_id}'")
                recipes = {k: r for k, r in recipes.items() if k == self.recipe_id}

            if self.prune:
                # Prune recipes to only run on certain items if we are asked to
                if hasattr(next(iter(recipes.values())), "copy_pruned"):
                    # pangeo-forge-recipes version < 0.10 has a `copy_pruned` method
                    recipes = {k: r.copy_pruned() for k, r in recipes.items()}

            bakery: Bakery = self.bakery_class(parent=self)

            # Check for a requirements.txt file and send it to beam if we have one
            requirements_path = feedstock.feedstock_dir / "requirements.txt"
            extra_options = {}
            if requirements_path.exists():
                extra_options["requirements_file"] = str(requirements_path)

            for name, recipe in recipes.items():
                pipeline_options = bakery.get_pipeline_options(
                    job_name=self.job_name,
                    # FIXME: Bring this in from meta.yaml?
                    container_image=self.container_image,
                    extra_options=extra_options,
                )

                # Set argv explicitly to empty so Apache Beam doesn't try to parse the commandline
                # for pipeline options - we have traitlets doing that for us.
                pipeline = Pipeline(options=pipeline_options, argv=[])
                # Chain our recipe to the pipeline. This mutates the `pipeline` object!
                # We expect `recipe` to either be a beam PTransform, or an object with a 'to_beam'
                # method that returns a transform.
                if isinstance(recipe, PTransform):
                    # This means we are in pangeo-forge-recipes >=0.9
                    pipeline | recipe
                elif hasattr(recipe, "to_beam"):
                    # We are in pangeo-forge-recipes <=0.9
                    # The import has to be here, as this import is not valid in pangeo-forge-recipes>=0.9
                    # NOTE: `StorageConfig` only requires a target; input and metadata caches are optional,
                    # so those are handled conditionally if provided.
                    from pangeo_forge_recipes.storage import StorageConfig

                    recipe.storage_config = StorageConfig(
                        target_storage.get_forge_target(job_name=self.job_name),
                    )
                    for attrname, optional_storage in zip(
                        ("cache", "metadata"),
                        (input_cache_storage, metadata_cache_storage),
                    ):
                        # `.root_path` is an empty string by default, so if the user has not setup this
                        # optional storage type in config, this block is skipped.
                        if optional_storage.root_path:
                            setattr(
                                recipe.storage_config,
                                attrname,
                                optional_storage.get_forge_target(
                                    job_name=self.job_name
                                ),
                            )
                    # with configured storage now attached, compile recipe to beam
                    pipeline | recipe.to_beam()

                # Some bakeries are blocking - if Beam is configured to use them, calling
                # pipeline.run() blocks. Some are not. We handle that here, and provide
                # appropriate feedback to the user too.
                extra = {"recipe": name, "job_name": self.job_name}
                if bakery.blocking:
                    self.log.info(
                        f"Running job for recipe {name}\n",
                        extra=extra | {"status": "running"},
                    )
                    pipeline.run()
                else:
                    result = pipeline.run()
                    job_id = result.job_id()
                    self.log.info(
                        f"Submitted job {job_id} for recipe {name}",
                        extra=extra | {"job_id": job_id, "status": "submitted"},
                    )
