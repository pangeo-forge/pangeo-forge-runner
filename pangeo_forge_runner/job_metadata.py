"""
Metadata related to a `bake` run
"""

from dataclasses import dataclass, asdict

@dataclass
class JobMetadata:
    recipe_name: str
    job_name: str

    def to_dict(self) -> dict:
        return asdict(self)