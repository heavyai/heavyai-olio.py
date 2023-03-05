#!/usr/bin/env python

from setuptools import setup

setup(
    name="heavyai-olio",
    version="0.1.0",
    description="HEAVY.AI DB: Python functions using the heavyai python library, Ibis and integration with various tools and datasets",
    author="Calvin Goodrich",
    author_email="calvin.goodrich@heavyai.com",
    url="https://github.com/heavyai/heavyai-olio.py",
    packages=[
        "heavyai_olio.pymapd",
        "heavyai_olio.ibis",
        "heavyai_olio.ipython",
        "heavyai_olio.catalog",
        "heavyai_olio.monitor",
        "heavyai_olio.cli",
        "heavyai_olio.schema",
        "heavyai_olio.workflow",
        "heavyai_olio.dashboard",
    ],
    install_requires=[
        "pandas",
        "heavyai >= 1.0",
        "ibis_heavyai >= 1.1",
        "sqlalchemy-heavyai >= 1.0",
        "prefect >= 2.2",
    ],
)
