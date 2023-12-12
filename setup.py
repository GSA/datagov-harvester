from setuptools import setup, find_packages
from codecs import open  # To use a consistent encoding
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the relevant file
with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="datagov-harvesting-logic",
    version="0.0.1",
    description="Data.gov metadata harvesting pipeline logic",
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Programming Language :: Python :: 3",
    ],  # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
    keywords="",
    author="Data.gov",
    author_email="datagovhelp@gsa.gov",
    url="https://github.com/GSA/datagov-harvesting-logic/",
    license="Public Domain",
    packages=find_packages(exclude=["ez_setup", "examples", "tests"]),
    namespace_packages=["harvester"],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        # TODO anything?
    ],
    setup_requires=["wheel"],
    entry_points="""
        # TODO anything?
    """,
)
