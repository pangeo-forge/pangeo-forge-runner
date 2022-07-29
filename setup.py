from setuptools import find_packages, setup

with open('README.md') as f:
    readme = f.read()

setup(
    name='pangeo-forge-runner',
    description="Commandline tool to manage pangeo-forge feedstocks",
    long_description=readme,
    long_description_content_type="text/markdown",
    author="Yuvi Panda",
    author_email="yuvipanda@gmail.com",
    version='0.2',
    packages=find_packages(),
    install_requires=[
        'jupyter-repo2docker',
        'ruamel.yaml',
        'pangeo-forge-recipes',
        'traitlets'
    ],
    entry_points={
        'console_scripts': ['pangeo-forge-runner=pangeo_forge_runner.__main__:main']
    }
)