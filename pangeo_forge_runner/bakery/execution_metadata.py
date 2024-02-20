from dataclasses import asdict, dataclass


@dataclass
class ExecutionMetadata:
    """
    Holds metadata for an execution instance, including recipe and job names.

    Attributes:
        recipe_name (str): Name of the recipe being executed.
        job_name (str): Unique name for the job execution.
    """

    recipe_name: str
    job_name: str

    def to_dict(self) -> dict:
        return asdict(self)
