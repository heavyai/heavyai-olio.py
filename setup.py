#!/usr/bin/env python

from setuptools import setup

setup(
    name="omnisci-olio",
    version="0.2.0.dev2",
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
    ],
    install_requires=[
        "pandas",
        # TODO we should specify min versions, but these, when mamba-installed report 0.0.0 to pip
        "pyomnisci",  # >= 0.27.0
        "pyomniscidb",  #  >= 5.6.4.1
        "ibis_omniscidb",  # >= 2.0.1
        # "ibis_omniscidb @ git+git://github.com/omnisci/ibis-omniscidb.git@master#egg=ibis-omniscidb",
        "sqlalchemy-omnisci",
        "prefect >= 0.15",
        # TODO these are dependencies of pyomnisci, not installed by that yet
        "netifaces",
        "thriftpy2",
    ],
)
