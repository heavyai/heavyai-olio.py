import os
import logging
import glob
import pandas as pd
from heavyai_olio.pymapd import copy_from


def logger():
    return logging.getLogger("heavyai_olio_loader")


geo_dir = "/heavyai/ThirdParty/geo_samples"


def heavyai_geo(con, table_name, src_file, drop=False, src_dir=geo_dir):
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


def heavyai_states(con, drop=False, src_dir=geo_dir, table_name="heavyai_states"):
    """
    If the table does not exists, loads from geojson file included with HEAVYAI installation.
    Returns Ibis table for HEAVYAI_STATES.
    """
    return heavyai_geo(
        con,
        drop=drop,
        src_dir=src_dir,
        table_name=table_name,
        src_file="us-states.json",
    )


def heavyai_counties(con, drop=False, src_dir=geo_dir, table_name="heavyai_counties"):
    """
    If the table does not exists, loads from geojson file included with HEAVYAI installation.
    Returns Ibis table for HEAVYAI_COUNTIES.
    """
    return heavyai_geo(
        con,
        drop=drop,
        src_dir=src_dir,
        table_name=table_name,
        src_file="us-counties.json",
    )


def heavyai_countries(con, drop=False, src_dir=geo_dir, table_name="heavyai_countries"):
    """
    If the table does not exists, loads from geojson file included with HEAVYAI installation.
    Returns Ibis table for HEAVYAI_COUNTRIES.
    """
    return heavyai_geo(
        con,
        drop=drop,
        src_dir=src_dir,
        table_name=table_name,
        src_file="countries.json",
    )


def heavydb_log(
    con,
    drop=False,
    src_dir="/storage/log",
    src_pattern="heavydb.INFO.*.log",
    table_name="heavydb_log",
    max_reject=100000000,
    max_rows=2 ** 32,
    ignore_errors=False,
    skip_older_files=True,
):
    """
    Loads stdlog lines from HEAVYAI DB server log files.
    Returns Ibis table for HEAVYDB_LOG.
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
