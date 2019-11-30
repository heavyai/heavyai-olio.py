# OmniSci-olio.py

This is a collection of python functions to use with OmniSci DB.


## Pymapd

`omnisci.pymapd` includes functions to connect and API functions over thrift.
Some of these may be moved to the upstream [pymapd](https://github.com/omnisci/pymapd).


## Ibis

`omnisci.ibis` includes functions to connect using prompts and session_id.


## IPython

Jupyter magic for `%%sql` when connected to OmniSciDB.

Usage in Jupyter:

```
%load_ext omnisci.ipython
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

## Loader

`omnisci.loader` includes functions to load standard datasets into OmniSciDB:

- `omnisci_states`
- `omnisci_counties`
- `omnisci_countries`
- OmniSciDB log files

