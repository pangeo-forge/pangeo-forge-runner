# represents the apache-beam type that
# Bakery.get_pipeline_options() should return
# https://github.com/apache/beam/blob/master/sdks/python/apache_beam/options/pipeline_options.py
# NOTE: we don't want apache-beam to be a dependency of pangeo-forge-runner
# but rather a dependency of the recipe that runner will install during `pangeo-forge-runner bake`
# workflow
ApacheBeamPipelineOptions = object
