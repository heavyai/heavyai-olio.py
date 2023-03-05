# HEAVYAI-olio.py

This is a collection of python functions to use with HEAVYAI DB.

[![Stability Experimental](https://img.shields.io/badge/stability-experimental-red.svg)](https://img.shields.io/badge/stability-experimental-red.svg)


## Schema

Python object API for generating table DDL.

For example, see [tests/test_schema.py](tests/test_schema.py).


## Workflow Client

High-level API client for workflow, with functions to connect, store data and [Prefect](https://docs.prefect.io/) tasks.

For example, see [tests/test_client.py](tests/test_client.py).


## Ibis and heavyai

See [Ibis project](https://docs.ibis-project.org/).

See also [heavyai project](https://github.com/heavyai/heavyai) and [docs](https://heavyai.readthedocs.io/en/latest/).

`heavyai_olio.ibis` includes functions to connect using prompts and session_id
and other functions on top of Ibis.


## IPython

Jupyter magic for `%%sql` when connected to HEAVYAI DB.

Usage in Jupyter:

```
%load_ext heavyai_olio.ipython
```

Then in a new cell:

```sql
%%sql
SELECT *
FROM heavyai_countries
LIMIT 10
```

## Monitor

Monitor system resources, cpu, disk, gpu/nvidia-smi, and also HEAVYAI DB internal memory.
Metrics can be saved to csv and/or loaded into HEAVYAI DB.


## Catalog

`heavyai_olio.catalog` includes functions to load standard datasets into OmniSciDB:

- `heavyai_states`
- `heavyai_counties`
- `heavyai_countries`
- HEAVYAI DB log files

For example, see (tests/test_catalog.py)[tests/test_catalog.py].

For more advance loading of HEAVYAI log files, see [heavyai-log-scraper](https://github.com/heavyai/log-scraper).


## Development, Test and Contribute

In general, we will use the same standards and guidelines as
[Heavyai contributing](https://heavyai.readthedocs.io/en/latest/contributing.html).

Some of the common commands are coded in the [Makefile](Makefile).
