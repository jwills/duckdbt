#!/usr/bin/env python
import os

from setuptools import find_namespace_packages
from setuptools import setup

this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, "README.md")) as f:
    long_description = f.read()

package_name = "duckdbt"


package_version = "0.5.0"
description = """The Modern Data Stack in a Python Package"""

setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Josh Wills",
    author_email="joshwills+duckdbt@gmail.com",
    url="https://github.com/jwills/duckdbt",
    packages=find_namespace_packages(include=["duckdbt", "duckdbt.*"]),
    include_package_data=True,
    install_requires=[
        "buenavista[duckdb]~=0.4.0",
        "dbt-duckdb>=1.5.0",
        "duckdb>=0.8.0",
        "fastapi[all]>=0.80.0,<1.0.0",
        "psycopg2-binary",
        "duckdb-engine",
    ],
)
