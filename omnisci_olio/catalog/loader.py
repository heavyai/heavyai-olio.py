import os
import logging
import glob
import pandas as pd
from omnisci_olio.pymapd import copy_from


def logger():
    return logging.getLogger("omnisci_olio_loader")


geo_dir = "/omnisci/ThirdParty/geo_samples"


def omnisci_geo(con, table_name, src_file, drop=False, src_dir=geo_dir):
    if table_name in con.list_tables():
        t = con.table(table_name)
        if drop:
            con.drop_table(table_name)
        else:
            return t
    q = f"""COPY {table_name} FROM '{src_dir}/{src_file}' WITH ( source_type='geo_file', max_reject=0 )"""
    logger().info(q)
    logger().info(copy_from(con.con, q))
    return con.table(table_name)


def omnisci_states(con, drop=False, src_dir=geo_dir, table_name="omnisci_states"):
    """
    If the table does not exists, loads from geojson file included with OmniSci installation.
    Returns Ibis table for OMNISCI_STATES.
    """
    return omnisci_geo(
        con,
        drop=drop,
        src_dir=src_dir,
        table_name=table_name,
        src_file="us-states.json",
    )


def omnisci_counties(con, drop=False, src_dir=geo_dir, table_name="omnisci_counties"):
    """
    If the table does not exists, loads from geojson file included with OmniSci installation.
    Returns Ibis table for OMNISCI_COUNTIES.
    """
    return omnisci_geo(
        con,
        drop=drop,
        src_dir=src_dir,
        table_name=table_name,
        src_file="us-counties.json",
    )


def omnisci_countries(con, drop=False, src_dir=geo_dir, table_name="omnisci_countries"):
    """
    If the table does not exists, loads from geojson file included with OmniSci installation.
    Returns Ibis table for OMNISCI_COUNTRIES.
    """
    return omnisci_geo(
        con,
        drop=drop,
        src_dir=src_dir,
        table_name=table_name,
        src_file="countries.json",
    )


def omnisci_log(
    con,
    drop=False,
    src_dir="/omnisci-storage/data/mapd_log",
    src_pattern="omnisci_server.INFO.*.log",
    table_name="omnisci_log",
    max_reject=100000000,
    max_rows=2 ** 32,
    ignore_errors=False,
    skip_older_files=True,
):
    """
    Loads stdlog lines from OmniSci DB server log files.
    Returns Ibis table for OMNISCI_LOG.
    """

    if table_name in con.list_tables():
        t = con.table(table_name)
        if drop:
            t.drop()

    if not table_name in con.list_tables():
        ddl = f"""CREATE TABLE {table_name}
            ( tstamp TIMESTAMP(9)
            , severity CHAR(1)
            , pid INTEGER
            , fileline TEXT ENCODING DICT(16)
            , label TEXT ENCODING DICT(16)
            , func TEXT ENCODING DICT(16)
            , matchid BIGINT
            , dur_ms BIGINT
            , dbname TEXT ENCODING DICT(16)
            , username TEXT ENCODING DICT(16)
            , pubsessid TEXT ENCODING DICT(16)
            , varnames TEXT[] ENCODING DICT(32)
            , varvalues TEXT[] ENCODING DICT(32)
            )
            WITH ( max_rows={max_rows}, sort_column='tstamp' )
        """
        logger().info(ddl)
        logger().info(con.con.execute(ddl).fetchall())

    t = con.table(table_name)

    if os.path.exists(src_dir):
        for path in glob.glob(f"{src_dir}/{src_pattern}"):
            try:
                # log files can have bad binary data
                with open(path, "rb") as f:
                    line = f.read(26)
                tstamp = pd.to_datetime(line.decode())
                tstamp = tstamp.ceil("s")
                if skip_older_files:
                    ct = t[t.tstamp >= tstamp].count().execute()
                    logger().info("%s %s %s", path, tstamp, ct)
                else:
                    ct = -1
                if ct == 0:
                    q = f"""COPY {table_name} FROM '{path}' WITH ( header='false', delimiter=' ', max_reject={max_reject}, threads=1 )"""
                    logger().debug(q)
                    logger().info(copy_from(con.con, q))

            except Exception as e:
                if ignore_errors:
                    logger().warning("skip %s %s", path, e)
                    continue
                else:
                    raise
    else:
        q = f"""COPY {table_name} FROM '{src_dir}/{src_pattern}' WITH ( header='false', delimiter=' ', max_reject={max_reject}, threads=1 )"""
        logger().info(q)
        logger().info(copy_from(con.con, q))

    return t
