from setuptools import find_packages, setup

with open("README.md") as f:
    readme = f.read()

setup(
    name="pangeo-forge-runner",
    description="Commandline tool to manage pangeo-forge feedstocks",
    long_description=readme,
    long_description_content_type="text/markdown",
    author="Yuvi Panda",
    author_email="yuvipanda@gmail.com",
    version="0.7.2",
    packages=find_packages(),
    python_requires=">=3.9",
    install_requires=[
        "jupyter-repo2docker",
        "ruamel.yaml",
        "pangeo-forge-recipes>=0.10.3",
        "escapism",
        "traitlets",
        # Matches the version of apache_beam in the default image,
        # specified in bake.py's container_image traitlet default
        "apache-beam[gcp]==2.47.0",
    ],
    entry_points={
        "console_scripts": ["pangeo-forge-runner=pangeo_forge_runner.cli:main"]
    },
)
