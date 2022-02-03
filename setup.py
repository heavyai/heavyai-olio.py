#!/usr/bin/env python

from setuptools import setup

setup(
    name="omnisci-olio",
    version="0.2.0.dev5",
    description="OmniSci DB: Python functions using pyomnici, Ibis and integration with various tools and datasets",
    author="Mike Hinchey",
    author_email="mike.hinchey@omnisci.com",
    url="https://github.com/omnisci/omnisci-olio.py",
    packages=[
        "omnisci_olio.pymapd",
        "omnisci_olio.ibis",
        "omnisci_olio.ipython",
        "omnisci_olio.catalog",
        "omnisci_olio.monitor",
        "omnisci_olio.cli",
        "omnisci_olio.schema",
        "omnisci_olio.workflow",
        "omnisci_olio.dashboard",
    ],
    install_requires=[
        "pandas",
        "pyomnisci >= 0.28.2",
        "pyomniscidb >= 5.8.0",
        "ibis_omniscidb >= 2.0.4",
        "sqlalchemy-omnisci >= 0.1.3",
        "prefect >= 0.15",
    ],
)
