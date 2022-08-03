from traitlets.config import LoggingConfigurable
from traitlets import Type, Dict, Unicode
from fsspec import AbstractFileSystem
from pangeo_forge_recipes.storage import (
    CacheFSSpecTarget,
    FSSpecTarget,
    MetadataTarget,
)


class StorageTargetConfig(LoggingConfigurable):
    fsspec_class = Type(
        klass=AbstractFileSystem,
        config=True,
        help="""
        FSSpec Filesystem to instantiate as class for this target
        """
    )

    fsspec_args = Dict(
        {},
        config=True,
        help="""
        Args to pass to fsspec_class during instantiation
        """
    )

    root_path = Unicode(
        "",
        config=True,
        help="""
        Root path under which to put all our storage.

        If {job_name} is present in the root_path, it will be expanded to the
        unique job_name of the current job.
        """
    )

    pangeo_forge_target_class = Type(
        config=False,
        help="""
        pangeo-forge-recipes class to instantiate
        """
    )

    def get_forge_target(self, job_name: str):
        """
        Return correct pangeo-forge-recipes Target

        If {job_name} is present in `root_path`, it is expanded with the given job_name
        """
        return self.pangeo_forge_target_class(
            self.fsspec_class(**self.fsspec_args),
            root_path=self.root_path.format(job_id=job_name)
        )

    def __str__(self):
        # Only show keys and type of values of args to fsspec, as they might contain secrets
        fsspec_args_filtered = ', '.join(f'{k}=<{type(v).__name__}>' for k, v in self.fsspec_args.items())
        return f'{self.pangeo_forge_target_class.__name__}({self.fsspec_class.__name__}({fsspec_args_filtered}, root_path="{self.root_path}")'


class TargetStorage(StorageTargetConfig):
    pangeo_forge_target_class = FSSpecTarget

class InputCacheStorage(StorageTargetConfig):
    pangeo_forge_target_class = CacheFSSpecTarget

class MetadataCacheStorage(StorageTargetConfig):
    pangeo_forge_target_class = MetadataTarget
