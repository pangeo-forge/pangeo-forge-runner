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
        Root path under which to put all our storage
        """
    )

    pangeo_forge_target_class = Type(
        config=False,
        help="""
        pangeo-forge-recipes class to instantiate
        """
    )

    def get_forge_target(self, job_id):
        """
        Return correct pangeo-forge-recipes Target
        """
        return self.pangeo_forge_target_class(
            self.fsspec_class(**self.fsspec_args),
            root_path=self.root_path.format(job_id=job_id)
        )


class TargetStorage(StorageTargetConfig):
    pangeo_forge_target_class = FSSpecTarget

class InputCacheStorage(StorageTargetConfig):
    pangeo_forge_target_class = CacheFSSpecTarget

class MetadataCacheStorage(StorageTargetConfig):
    pangeo_forge_target_class = MetadataTarget
