from setuptools import find_packages, setup

with open("README.md") as f:
    readme = f.read()

setup(
    description="Commandline tool to manage pangeo-forge feedstocks",
    long_description=readme,
    long_description_content_type="text/markdown",
    author="Yuvi Panda",
    author_email="yuvipanda@gmail.com",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[
        "jupyter-repo2docker",
        "ruamel.yaml",
        "pangeo-forge-recipes>=0.9.2",
        "escapism",
        "jsonschema",
        "traitlets",
        "importlib-metadata",
        # Matches the version of apache_beam in the default image,
        # specified in bake.py's container_image traitlet default
        "apache-beam[gcp]==2.42.0",
    ],
    extras_require={
        "dask": [
            # Pinning upper bound to `2023.9.2` as a workaround until the following fix goes in:
            # https://github.com/apache/beam/pull/27618/files#diff-bfb5ae715e9067778f492058e8a02ff877d6e7584624908ddbdd316853e6befbL102-R107
            "dask>=2022.6.0,<2023.9.3",
            "distributed>=2022.6.0,<2023.9.3",
        ],
    },
    entry_points={
        "console_scripts": ["pangeo-forge-runner=pangeo_forge_runner.cli:main"]
    },
)
