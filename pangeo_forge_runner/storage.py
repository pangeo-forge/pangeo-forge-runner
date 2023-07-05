from fsspec import AbstractFileSystem
from traitlets import Dict, Type, Unicode
from traitlets.config import LoggingConfigurable


class StorageTargetConfig(LoggingConfigurable):
    """
    Configuration for Storage Targets when a Pangeo Forge recipe is baked
    """

    fsspec_class = Type(
        klass=AbstractFileSystem,
        config=True,
        help="""
        FSSpec Filesystem to instantiate as class for this target
        """,
    )

    fsspec_args = Dict(
        {},
        config=True,
        help="""
        Args to pass to fsspec_class during instantiation
        """,
    )

    root_path = Unicode(
        "",
        config=True,
        help="""
        Root path under which to put all our storage.

        If {job_name} is present in the root_path, it will be expanded to the
        unique job_name of the current job.
        """,
    )

    pangeo_forge_target_class = Unicode(
        config=False,
        help="""
        Name of StorageConfig class from pangeo_forge_recipes to instantiate.

        Should be set by subclasses.
        """,
    )

    def get_forge_target(self, job_name: str):
        """
        Return correct pangeo-forge-recipes Target

        If {job_name} is present in `root_path`, it is expanded with the given job_name
        """
        # import dynamically on call, because different versions of `pangeo-forge-recipes.storage`
        # contain different objects, so a static top-level import cannot be used.
        from pangeo_forge_recipes import storage

        cls = getattr(storage, self.pangeo_forge_target_class)

        return cls(
            self.fsspec_class(**self.fsspec_args),
            root_path=self.root_path.format(job_name=job_name),
        )

    def __str__(self):
        """
        Return sanitized string representation, stripped of possible secrets
        """
        # Only show keys and type of values of args to fsspec, as they might contain secrets
        fsspec_args_filtered = ", ".join(
            f"{k}=<{type(v).__name__}>" for k, v in self.fsspec_args.items()
        )
        return f'{self.pangeo_forge_target_class}({self.fsspec_class.__name__}({fsspec_args_filtered}, root_path="{self.root_path}")'


class TargetStorage(StorageTargetConfig):
    """
    Storage configuration for where the baked data should be stored
    """

    pangeo_forge_target_class = "FSSpecTarget"


class InputCacheStorage(StorageTargetConfig):
    """
    Storage configuration for caching input files during recipe baking
    """

    pangeo_forge_target_class = "CacheFSSpecTarget"


class MetadataCacheStorage(StorageTargetConfig):
    """
    Storage configuration for caching metadata during recipe baking
    """

    pangeo_forge_target_class = "MetadataTarget"
