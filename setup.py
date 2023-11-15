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
    ],
    extras_require={
        "dataflow": ["apache-beam[gcp]"],
        "flink": ["apache-beam>=2.47.0"],
    },
    entry_points={
        "console_scripts": ["pangeo-forge-runner=pangeo_forge_runner.cli:main"]
    },
)
