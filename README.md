# OmniSci-olio.py

This is a collection of python functions to use with OmniSci DB.

[![Stability Experimental](https://img.shields.io/badge/stability-experimental-red.svg)](https://img.shields.io/badge/stability-experimental-red.svg)


## Schema

Python object API for generating table DDL.

For example, see [tests/test_schema.py](tests/test_schema.py).


## Workflow Client

High-level API client for workflow, with functions to connect, store data and [Prefect](https://docs.prefect.io/) tasks.

For example, see [tests/test_client.py](tests/test_client.py).


## Ibis and Pyomnisci

See [Ibis project](https://docs.ibis-project.org/).

See also [Pyomnisci project](https://github.com/omnisci/pymapd) and [docs](https://pyomnisci.readthedocs.io/en/latest/).

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

For example, see (tests/test_catalog.py)[tests/test_catalog.py].

For more advance loading of omnisci log files, see [omnisci-log-scraper](https://github.com/omnisci/log-scraper).


## Development, Test and Contribute

In general, we will use the same standards and guidelines as
[Pyomnisci contributing](https://pyomnisci.readthedocs.io/en/latest/contributing.html).

Some of the common commands are coded in the [Makefile](Makefile).
