#!/usr/bin/env python

from setuptools import setup

setup(name='omnisci-olio',
    version='0.1.0-dev0',
    description='OmniSci DB: Python functions using Pymapd, Ibis and integration with various tools and datasets',
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
        "pymapd",
        "ibis-framework",
        # "ibis-framework @ git+git://github.com/ibis-project/ibis.git@72ece317337fb7d329337f20db930845a669ce85#egg=ibis-framework",
    ]
)
