from setuptools import find_packages, setup

setup(
    name='pangeo-forge-runner',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        'jupyter-repo2docker',
        'ruamel.yaml'
    ],
    entry_points={
        'console_scripts': ['pangeo-forge-runner=pangeo_forge_runner.__main__:main']
    }
)