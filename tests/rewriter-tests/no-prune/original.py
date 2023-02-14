import apache_beam as beam
from pangeo_forge_recipes.patterns import pattern_from_file_sequence

pattern = pattern_from_file_sequence(["hi.nc"])

recipe = beam.Create(pattern.items())
