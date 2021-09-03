#!/usr/bin/env python

from setuptools import setup

setup(name='omnisci-olio',
    version='0.2.0.dev1',
    description='OmniSci DB: Python functions using pyomnici, Ibis and integration with various tools and datasets',
    author='Mike Hinchey',
    author_email='mike.hinchey@omnisci.com',
    url='https://github.com/omnisci/omnisci-olio.py',
    packages=[
        'omnisci_olio.pymapd',
        'omnisci_olio.ibis',
        'omnisci_olio.ipython',
        'omnisci_olio.catalog',
        'omnisci_olio.monitor',
        'omnisci_olio.cli',
        ],
    install_requires=[
        "pandas",
        "pyomnisci>=0.27.0",
        "pyomniscidb>=5.6.4.1",
        "ibis_omniscidb>=0.2.0",
    ]
)
