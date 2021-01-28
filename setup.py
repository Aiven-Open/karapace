"""
karapace - setup

Copyright (c) 2019 Aiven Ltd
See LICENSE for details
"""
from setuptools import find_packages, setup

import os
import version

readme_path = os.path.join(os.path.dirname(__file__), "README.rst")
with open(readme_path, "r") as fp:
    readme_text = fp.read()

version_for_setup_py = version.get_project_version("karapace/version.py")
version_for_setup_py = ".dev".join(version_for_setup_py.split("-", 2)[:2])

setup(
    name="karapace",
    version=version_for_setup_py,
    zip_safe=False,
    packages=find_packages(exclude=["test"]),
    install_requires=["aiohttp", "accept-types"],
    extras_require={},
    dependency_links=[],
    package_data={},
    entry_points={
        "console_scripts": [
            "karapace = karapace.karapace_all:main",
            "karapace_schema_backup = karapace.schema_backup:main",
        ],
    },
    author="Hannu Valtonen",
    author_email="hannu.valtonen@aiven.io",
    license="Apache 2.0",
    platforms=["POSIX", "MacOS"],
    description="Karapace",
    long_description=readme_text,
    url="https://github.com/aiven/karapace/",
    python_requires=">=3.7",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Database :: Database Engines/Servers",
        "Topic :: Software Development :: Libraries",
    ],
)
