"""
karapace - setup
Copyright (c) 2023 Aiven Ltd
See LICENSE for details
"""

from setuptools import Extension, setup

setup(
    setup_requires=["setuptools-golang"],
    ext_modules=[
        Extension(
            "protopacelib",
            ["go/protopace/main.go"],
        ),
    ],
    build_golang={"root": "go/protopace"},
)
