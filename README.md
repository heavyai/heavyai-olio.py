# OmniSci-olio.py

This is a collection of python functions to use with OmniSci DB.

[![Stability Experimental](https://img.shields.io/badge/stability-experimental-red.svg)](https://img.shields.io/badge/stability-experimental-red.svg)


## Pymapd

See [Pymapd project](https://github.com/omnisci/pymapd) and [docs](https://pymapd.readthedocs.io/en/latest/).

`omnisci_olio.pymapd` includes functions to connect and API functions over thrift.
Some of these may be moved to the upstream pymapd project.


## Ibis

See [Ibis project](https://docs.ibis-project.org/).

`omnisci_olio.ibis` includes functions to connect using prompts and session_id
and other functions on top of Ibis.


## IPython

Jupyter magic for `%%sql` when connected to OmniSciDB.

Usage in Jupyter:

```
%load_ext omnisci_olio.ipython
```

Then in a new cell:

```sql
%%sql
SELECT *
FROM omnisci_countries
LIMIT 10
```

## Monitor

Monitor system resources, cpu, disk, gpu/nvidia-smi, and also OmniSciDB internal memory.
Metrics can be saved to csv and/or loaded into OmniSciDB.


## Catalog

`omnisci_olio.catalog` includes functions to load standard datasets into OmniSciDB:

- `omnisci_states`
- `omnisci_counties`
- `omnisci_countries`
- OmniSciDB log files


## Development, Test and Contribute

In general, we will use the same standards and guidelines as [Pymapd contributing](https://pymapd.readthedocs.io/en/latest/contributing.html).

Some of the common commands are coded in the [Makefile](Makefile).

