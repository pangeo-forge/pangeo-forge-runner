"""
Command to run a pangeo-forge recipe
"""

import hashlib
import os
import re
import string
import time
from importlib.metadata import distributions
from pathlib import Path

import escapism
from apache_beam import Pipeline, PTransform
from traitlets import Bool, Type, Unicode, validate

from .. import Feedstock
from ..bakery.base import Bakery
from ..bakery.flink import FlinkOperatorBakery
from ..bakery.local import LocalDirectBakery
from ..plugin import get_injections, get_injectionspecs_from_entrypoints
from ..storage import InputCacheStorage, TargetStorage
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
        "",
        config=True,
        help="""
        Container image to use for this job.

        For GCP DataFlow leaving it blank defaults to letting beam 
        automatically figure out the image to use for the workers 
        based on version of beam and python in use.
        
        For Flink it's required that you pass an beam image
        for the version of python and beam you are targeting
        for example: apache/beam_python3.10_sdk:2.51.0
        more info: https://hub.docker.com/layers/apache/

        Note that some runners (like the local one) may not support this!
        """,
    )

    @validate("container_image")
    def _validate_container_image(self, proposal):
        if self.bakery_class == FlinkOperatorBakery and not proposal.value:
            raise ValueError(
                "'container_name' is required when using the 'FlinkOperatorBakery' "
                "for the version of python and apache-beam you are targeting. "
                "See the sdk images available: https://hub.docker.com/layers/apache/"
            )
        return proposal.value

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
        if not "pangeo-forge-recipes" in [d.metadata["Name"] for d in distributions()]:
            raise ValueError(
                "To use the `bake` command, `pangeo-forge-recipes` must be installed."
            )
        # Create our storage configurations. Traitlets will do its magic, populate these
        # with appropriate config from config file / commandline / defaults.
        target_storage = TargetStorage(parent=self)
        input_cache_storage = InputCacheStorage(parent=self)

        self.log.info(
            f"Target Storage is {target_storage}\n", extra={"status": "setup"}
        )
        self.log.info(
            f"Input Cache Storage is {input_cache_storage}\n", extra={"status": "setup"}
        )

        injection_specs = get_injectionspecs_from_entrypoints()

        with self.fetch() as checkout_dir:
            if not self.job_name:
                self.job_name = self.autogenerate_job_name()

            injection_values = {
                "TARGET_STORAGE": target_storage.get_forge_target(
                    job_name=self.job_name
                ),
            }

            if not input_cache_storage.is_default():
                cache_target = input_cache_storage.get_forge_target(
                    job_name=self.job_name
                )
                injection_values |= {"INPUT_CACHE_STORAGE": cache_target}

            feedstock = Feedstock(
                Path(checkout_dir) / self.feedstock_subdir,
                prune=self.prune,
                callable_args_injections=get_injections(
                    injection_specs, injection_values
                ),
            )

            self.log.info("Parsing recipes...", extra={"status": "running"})
            with (
                redirect_stderr(self.log, {"status": "running"}),
                redirect_stdout(self.log, {"status": "running"}),
            ):
                recipes = feedstock.parse_recipes()

            if self.recipe_id:
                if self.recipe_id not in recipes:
                    raise ValueError(f"{self.recipe_id=} not in {list(recipes)}")
                self.log.info(f"Baking only recipe_id='{self.recipe_id}'")
                recipes = {k: r for k, r in recipes.items() if k == self.recipe_id}

            bakery: Bakery = self.bakery_class(parent=self)

            extra_options = {}

            for name, recipe in recipes.items():
                if hasattr(recipe, "to_beam"):
                    # Catch recipes following pre-0.10 conventions and throw
                    raise ValueError(
                        "Unsupported recipe: please update to support pfr >=0.10 conventions."
                    )

                if len(recipes) > 1:
                    recipe_name_hash = hashlib.sha256(name.encode()).hexdigest()[:5]
                    per_recipe_unique_job_name = (
                        self.job_name[: 62 - 6] + "-" + recipe_name_hash
                    )
                    self.log.info(
                        f"Deploying > 1 recipe. Modifying base {self.job_name = } for recipe "
                        f"{name = } with {recipe_name_hash = }. Submitting job with modified "
                        f"{per_recipe_unique_job_name = }. Note: job names must be <= 63 chars. "
                        "If job_name was > 57 chars, it was truncated to accomodate modification."
                    )
                else:
                    per_recipe_unique_job_name = None

                requirements_path = feedstock.feedstock_dir / "requirements.txt"
                if requirements_path.exists():
                    extra_options["requirements_file"] = str(requirements_path)

                pipeline_options = bakery.get_pipeline_options(
                    job_name=(per_recipe_unique_job_name or self.job_name),
                    # FIXME: Bring this in from meta.yaml?
                    container_image=self.container_image,
                    extra_options=extra_options,
                )

                # Set argv explicitly to empty so Apache Beam doesn't try to parse the commandline
                # for pipeline options - we have traitlets doing that for us.
                pipeline = Pipeline(options=pipeline_options, argv=[])

                # Chain our recipe to the pipeline. This mutates the `pipeline` object!
                # We expect `recipe` to be 1) a beam PTransform or 2) or a a string that leverages the
                # `dict_object:` see `tests/test-data/gpcp-from-gcs/feedstock-0.10.x-dictobj/meta.yaml`
                # as an example
                if isinstance(recipe, PTransform):
                    pipeline | recipe

                # Some bakeries are blocking - if Beam is configured to use them, calling
                # pipeline.run() blocks. Some are not. We handle that here, and provide
                # appropriate feedback to the user too.
                extra = {
                    "recipe": name,
                    "job_name": (per_recipe_unique_job_name or self.job_name),
                }
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
