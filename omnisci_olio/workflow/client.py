import os
import sys
from time import time

# import logging
import pathlib
import hashlib
import datetime
import pandas as pd
from sqlalchemy.engine.url import make_url

import prefect

import ibis
import ibis_omniscidb

import omnisci_olio.schema as sc
from omnisci_olio.ibis import connect as ibis_connect

try:
    from ibis_omniscidb import Backend as OmniSciDBBackend
except:
    # pre-2.0 ibis and ibis_omniscidb
    from ibis_omniscidb import OmniSciDBClient as OmniSciDBBackend


def logger():
    # return logging.getLogger('default')
    return prefect.context.get("logger")


def log_info(**kwargs):
    logger().info(str(kwargs))


def log_warning(**kwargs):
    logger().warning(str(kwargs))


def log_error(**kwargs):
    logger().error(str(kwargs))


# TODO deprecate in favor of log_info
def logi(**kwargs):
    logger().info(str(kwargs))


def log_infx(
    cmd,
    tstart,
    target=None,
    sources=None,
    src_ct=None,
    ct_before=0,
    ct_after=0,
    sql=None,
    tend=None,
    **kwargs,
):
    tend = tend or time()
    sources = list(set(sources)) if sources else None
    logi(
        cmd=cmd,
        time_s=round(tend - tstart, 2),
        target=target,
        src_ct=src_ct,
        ct_before=ct_before,
        ct_after=ct_after,
        sources=sources,
        sql=sql,
        **kwargs,
    )


def _file_md5sum(filename):
    h = hashlib.md5()
    with open(filename, "rb") as f:
        chunk = f.read(4096)
        while chunk:
            h.update(chunk)
            chunk = f.read(4096)
    return h


def sqlpp(x):
    try:
        import sqlparse

        if isinstance(x, str):
            print(sqlparse.format(x, reindent=True))
        else:
            print(sqlparse.format(x.compile(), reindent=True))
    except:
        if isinstance(x, str):
            print(x)
        else:
            print(x.compile())


def _schema_apply_to(schema, df):
    """
    With Pandas 1.0, int32 does not support None, so the Ibis apply_to is not effectively fixing the column type.
    With OmniSci 5.6, input validate is more strict, so "1.0" fails to load as an INT.
    This converts to Int32 instead of int32 (and other int sizes).
    """
    df = schema.apply_to(df)
    for column, dtype in schema.items():
        pandas_dtype = dtype.to_pandas()
        col = df[column]
        col_dtype = col.dtype

        try:
            not_equal = pandas_dtype != col_dtype
        except TypeError:
            # ugh, we can't compare dtypes coming from pandas, assume not equal
            not_equal = True

        if not_equal:
            if isinstance(dtype, ibis.expr.datatypes.Int8):
                df[column] = df[column].astype("Int8", errors="ignore")
            elif isinstance(dtype, ibis.expr.datatypes.Int16):
                df[column] = df[column].astype("Int16", errors="ignore")
            elif isinstance(dtype, ibis.expr.datatypes.Int32):
                df[column] = df[column].astype("Int32", errors="ignore")
            elif isinstance(dtype, ibis.expr.datatypes.Int64):
                df[column] = df[column].astype("Int64", errors="ignore")

    return df


db_update_log_table = sc.Table(
    "omnisci_db_update_log",
    [
        sc.Column("created_at", sc.Timestamp(9)),
        sc.Column("created_by", sc.text16),
        sc.Column("hostname", sc.text16),
        sc.Column("database_name", sc.text16),
        sc.Column("omnisci_version", sc.text8),
        sc.Column("omnisci_session", sc.text32),
        sc.Column("src_app", sc.text16),
        sc.Column("src_paths", sc.Text(array=True)),
        sc.Column("src_tables", sc.Text(array=True)),
        sc.Column("process_ap", sc.Text(16)),
        sc.Column("operation", sc.Text(16)),
        sc.Column("command", sc.Text()),
        sc.Column("tgt_table", sc.Text(16)),
        sc.Column("process_time", sc.Timestamp(9)),
        sc.Column("process_sec", sc.Float()),
        sc.Column("process_rows", sc.Integer()),
        sc.Column("error_count", sc.Integer()),
        sc.Column("rows_before", sc.int64),
        sc.Column("rows_after", sc.int64),
        sc.Column("data_timestamp", sc.Timestamp(9)),
        sc.Column("update_key", sc.Text()),
        sc.Column("message", sc.Text()),
        sc.Column("task", sc.Text(16)),
        sc.Column("task_map_index", sc.Integer()),
    ],
    props=dict(fragment_size=1000000, max_rollback_epochs=3, max_rows=2000000),
)


class OmniSciDBClient:
    """
    If close_on_exit is False, don't automatically close the connection in a `with` block to not close other uses.
    """

    def __init__(
        self,
        uri=None,
        con=None,
        close_on_exit=True,
        _other=None,
        dryrun=False,
    ):
        self.close_on_exit = close_on_exit
        self.sources = []
        self.storing = None
        self.dryrun = dryrun

        if _other is not None:
            self.con = _other.con
        if con is not None:
            self.con = con
        else:
            uri = uri or os.environ.get("OMNISCI_DB_URL")
            if uri is None:
                raise Exception(
                    "A DB connection URL must be provided by one of: `con`, `uri` param, or env var `OMNISCI_DB_URL`"
                )
            self.con = ibis_connect(uri)

        self._log_inited = False
        self._log_data = None
        self._log_con = None

        if _other is not None:
            # to reducee the number of server calls
            self._log_data = _other._log_data
            self._log_con = _other._log_con
        else:
            log_uri = os.environ.get("OMNISCI_DB_LOG_URL", None)
            if log_uri:
                self._log_con = ibis_connect(log_uri)

                uri = make_url(self.con.uri)
                ss = self._server_status()
                si = self._session_info()
                self._log_data = (
                    si.user,
                    uri.host if uri.host else ss.host_name,
                    si.database,
                    ss.version,
                    self.con.con._session[:12],
                )

    def __enter__(self):
        # By not closing on exit, this connection can be used in nested `with connect() as con` blocks.
        # return OmniSciDBClient(_other=self, close_on_exit=self.close_on_exit)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.close_on_exit:
            self.con.__exit__(exc_type, exc_val, exc_tb)

    ########################
    # Utility functions
    ########################

    def clean_name(self, name, pretty=False):
        from sqlalchemy_omnisci.base import RESERVED_WORDS

        name = name.strip(" ")
        name = name.replace("%", "pct")
        name = name.replace("<=", "le")
        name = name.replace("<", "lt")
        name = name.replace(">=", "ge")
        name = name.replace(">", "gt")
        name = name.replace("+", "plus")
        # name = name.replace("-", "minus")
        a = "".join([c if c.isalnum() else "_" for c in name.strip()])
        b = (a + "_") if (a.upper() in RESERVED_WORDS) else a
        if b[0].isdigit():
            b = "_" + b
        if pretty:
            x = b.replace("__", "_")
            x = x.replace("__", "_")
            x = x.rstrip("_")
            return x.lower()
        else:
            return b

    def clean_names(self, columns, pretty=False):
        return [self.clean_name(c, pretty=pretty) for c in columns]

    def pp(self, x):
        try:
            import sqlparse

            print(sqlparse.format(self.to_sql(x), reindent=True))
        except:
            print(self.to_sql(x))

    def tmp_tablename(self, task, src_table, *args):
        args_str = "__" + "_".join(args) if args else ""
        return self.clean_name(f"tmp_{task}__{src_table}" + args_str)

    ########################
    # Ibis-related
    ########################

    def source(self, name):
        self.sources.append(name)
        return name

    def src_file(self, filename):
        stat = pathlib.Path(filename).stat()
        modtime = datetime.datetime.fromtimestamp(stat.st_mtime)
        md5sum = _file_md5sum(filename).hexdigest()
        logi(cmd="src_file", modtime=modtime, size=stat.st_size, md5sum=md5sum)

        self.sources.append(filename)
        return filename

    def src_table(self, t):
        if isinstance(t, ibis_omniscidb.client.OmniSciDBTable):
            # this reestablishes the table to be connected to self rather than some other (stale) connection
            self.sources.append(t.name)
            return self.con.table(t.name)
        else:
            self.sources.append(t)
            return self.con.table(t)

    def table(self, t):
        return self.src_table(t)

    def to_table(self, t):
        return self.src_table(t)

    def to_expr(self, x):
        if isinstance(x, ibis_omniscidb.client.OmniSciDBTable):
            return self.to_table(x)
        elif isinstance(x, ibis.expr.types.Expr):
            return x
        else:
            # assume a str is a table name
            return self.table(x)

    def to_sql(self, expr):
        if isinstance(expr, str):
            return expr
        else:
            return self.con.compile(expr)

    def _name(self, thing):
        if isinstance(thing, ibis_omniscidb.client.OmniSciDBTable):
            return thing.name
        elif isinstance(thing, ibis.expr.api.Expr):
            return thing.get_name()
        else:
            return str(thing)

    def _names(self, things):
        if isinstance(things, list):
            return [self._name(x) for x in things]
        else:
            return [self._name(things)]

    def exists_table(self, t):
        return t in self.con.list_tables()

    def count(self, t):
        return self.con.execute(self.table(t).count())

    ########################
    # DB LOG
    ########################

    def _log_init(self):
        if (
            not self._log_inited
            and self._log_con
            and db_update_log_table.name not in self._log_con.list_tables()
        ):
            self._log_inited = True
            self._log_con.con.execute(db_update_log_table.compile())

    def _server_status(self):
        return self.con.con._client.get_server_status(self.con.con._session)

    def _session_info(self):
        return self.con.con._client.get_session_info(self.con.con._session)

    def _insert_log(
        self,
        start_time,
        src_paths,
        operation,
        command,
        tgt_table,
        process_rows,
        error_count,
        rows_before,
        rows_after,
        data_timestamp,
        update_key,
        message,
        src_tables=None,
    ):
        if self._log_con is None:
            return
        tend = time()
        now = datetime.datetime.now()
        row = (
            now,
            self._log_data[0],
            self._log_data[1],
            self._log_data[2],
            self._log_data[3],
            self._log_data[4],
            None,  # src_app
            src_paths or [],
            src_tables or [],
            getattr(prefect.context, "flow_name", sys.argv[0]),
            operation,
            command,
            tgt_table,
            now,
            (tend - start_time),
            process_rows,
            error_count,
            rows_before,
            rows_after,
            data_timestamp,
            update_key,
            message,
            getattr(prefect.context, "task_name", None),
            getattr(prefect.context, "map_index", None),
        )
        self._log_init()
        # TODO load_table method=rows is inserting string 'None' instead of None/NULL
        self._log_con.con.load_table(db_update_log_table.name, [row], method="rows")

    def log(
        self,
        cmd,
        tstart,
        target=None,
        sources=None,
        ct_before=None,
        ct_after=None,
        process_rows=None,
        sql=None,
        tend=None,
        error_count=None,
        message=None,
        **kwargs,
    ):
        tend = tend or time()
        sources = list(set(self._names((sources or []) + self.sources)))
        msg = str(dict(
            cmd=cmd,
            time_s=round(tend - tstart, 2),
            target=target,
            ct_before=ct_before,
            ct_after=ct_after,
            sources=sources,
            process_rows=process_rows,
            sql=sql,
            error_count=error_count,
            **kwargs,
        ))
        if error_count and error_count > 0:
            logger().error(msg)
        else:
            logger().info(msg)
        self._insert_log(
            start_time=tstart,
            src_paths=None,
            operation=cmd,
            command=sql,
            tgt_table=target,
            process_rows=process_rows,
            error_count=error_count,
            rows_before=ct_before,
            rows_after=ct_after,
            data_timestamp=None,
            update_key=None,
            message=message,
            src_tables=sources,
        )

    ########################
    # Ibis
    ########################

    # def on_matching_columns(table_a_cols, table_b):
    #     """
    #     Create an Ibis join expression that is the AND of all table_a_cols equal to the same named columns in table_b.
    #     Return an expression.
    #     """
    #     res = None
    #     for col in table_a_cols:
    #         x = (col == table_b[col.get_name()])
    #         if res is None:
    #             res = x
    #         else:
    #             res = (res & x)
    #     return res

    ########################
    # Query and Update
    ########################

    def query(self, sql):
        sql = self.to_sql(sql)
        tstart = time()
        df = pd.read_sql(sql, self.con.con)
        time_s = time() - tstart
        if time_s > 2.0:
            # 2 seconds is sometimes a long time, but not sure this should be a warning
            logi(cmd="query", time_s=round(time_s, 2), sql=sql)
        return df

    def exec_update(self, table_name, sql, sources=None, cmd="execute_update"):
        sql = sql.strip().replace("\n", " ")
        if self.dryrun:
            sqlpp(sql)
        else:
            table_name = self._name(table_name)
            logi(cmd=cmd, target=table_name, sql=sql)

            before = 0
            if table_name in self.con.list_tables():
                try:
                    t = self.table(table_name)
                    before = self.con.execute(t.count())
                except Exception as e:
                    log_warning(exception=e)

            tstart = time()
            try:
                response = self.con.con.execute(sql).fetchall()
            except Exception as e:
                raise Exception(sql) from e
            tend = time()

            if response and len(response) > 0 and len(response[0]) > 0:
                msg = response[0][0]
                if msg.find("Failed") > -1:
                    # for example: "Loader Failed" from COPY FROM
                    raise Exception(
                        str(
                            dict(
                                cmd=cmd,
                                time_s=round(tend - tstart, 2),
                                table_name=table_name,
                                sources=sources,
                                before=before,
                                sql=sql,
                                response=response,
                            )
                        )
                    )

            after = 0
            if self.exists_table(table_name):
                t = self.table(table_name)
                after = self.con.execute(t.count())

            self.log(
                cmd,
                tstart,
                table_name,
                sources,
                before,
                after,
                sql=sql,
                response=response,
            )
        return table_name

    def insert_as(self, table, sql, sources=None):
        tn = self._name(table)
        return self.exec_update(
            tn,
            f"""INSERT INTO "{tn}" {self.to_sql(sql)};""",
            sources=sources,
            cmd="INSERT",
        )

    def _with_props(self, kwargs):
        if not kwargs:
            return ""

        def val(v):
            if isinstance(v, str):
                return f"'{v}'"
            elif isinstance(v, bool):
                return "'true'" if v else "'false'"
            else:
                return str(v)

        props = [f"{k}={val(v)}" for k, v in kwargs.items() if v is not None]
        if len(props) > 0:
            props = ", ".join(props)
            return f"WITH ({props})"
        else:
            return ""

    def explain(self, query, plan_type=""):
        """
        plan_type: "" (default), "PLAN", or "CALCITE"
        """
        return self.con.con.execute(f"EXPLAIN {plan_type} " + self.to_sql(query)).fetchone()[0]

    def show_create_table(self, tn):
        self.con.execute(f"SHOW CREATE TABLE {tn}").fetchone()[0]

    def create_table(self, table, ddl, drop=False):
        tn = self._name(table)
        if drop:
            self.drop_table(tn)
        ddl = ddl.compile(table) if isinstance(ddl, sc.Table) else ddl
        return self.exec_update(table, ddl)

    def create_table_as(self, table, sql, sources=None, drop=False, **kwargs):
        tn = self._name(table)
        if drop:
            self.drop_table(tn)
        props = self._with_props(kwargs)
        ctas = f"""CREATE TABLE {tn} AS (
{self.to_sql(sql)}
) {props};"""
        return self.exec_update(tn, ctas, sources=sources, cmd="CREATE TABLE AS")

    def create_view_as(self, target_name, sql, sources=None, drop=False, **kwargs):
        tn = self._name(target_name)
        if drop:
            self.drop_view(tn)
        props = self._with_props(kwargs)
        ctas = f"""CREATE VIEW {tn} AS (
{self.to_sql(sql)}
) {props};"""
        return self.exec_update(tn, ctas, sources=sources, cmd="CREATE VIEW")

    def drop_table(self, table_name):
        if self.exists_table(table_name):
            return self.exec_update(
                table_name, f"DROP TABLE IF EXISTS {table_name};", cmd="DROP TABLE"
            )
        return table_name

    def drop_view(self, table_name):
        if self.exists_table(table_name):
            return self.exec_update(
                table_name, f"DROP VIEW {table_name};", cmd="DROP VIEW"
            )
        return table_name

    def rename_table(self, from_table, to_table):
        return self.exec_update(
            to_table,
            f"""ALTER TABLE {from_table} RENAME TO {to_table}""",
            cmd="RENAME",
            sources=[from_table],
        )

    def exists_table_column(self, table, column):
        if self.exists_table(table):
            t = self.table(table)
            return column in t.columns
        return False

    def alter_table_rename_column(self, table, src_col, tgt_col):
        if self.exists_table(table):
            t = self.table(table)
            if src_col in t.columns:
                self.exec_update(
                    table,
                    f"""ALTER TABLE {table} RENAME COLUMN {src_col} TOD {tgt_col}""",
                    cmd="RENAME",
                    sources=[table],
                )

    def delete_where(self, table_name, where_sql):
        return self.exec_update(
            table_name, f"DELETE FROM {table_name} WHERE {where_sql};", cmd="DELETE"
        )

    def copy_from(self, table_name, from_file_glob, **kwargs):
        props = self._with_props(kwargs)
        q = f"""COPY "{table_name}" FROM '{from_file_glob}' {props};"""
        return self.exec_update(
            table_name, q, sources=[from_file_glob], cmd="COPY FROM"
        )

    def _shared_tmp_dir(self):
        path = "/jhub_omnisci_dropbox"
        if os.path.exists(path):
            path += "/tmp"
            os.makedirs(path, exist_ok=True)
            return path
        else:
            # TODO add a config option so this is more flexible/usable
            raise Exception(f"Path not found for shared tmp dir: {path}")

    def store_by_copy(self, df, tgt_table, ddl=None, sources=None, drop=False, key=""):
        tstart = time()
        fname = self.clean_name(
            "__".join(
                [
                    "store_by_copy",
                    tgt_table,
                    str(key),
                    datetime.datetime.now().isoformat(),
                ]
            )
        )
        tmp_file = f"{self._shared_tmp_dir()}/{fname}.csv"
        if os.path.exists(tmp_file):
            os.remove(tmp_file)
        if drop:
            self.drop_table(tgt_table)

        if ddl and not self.exists_table(tgt_table):
            self.create_table(tgt_table, ddl)

        if self.exists_table(tgt_table):
            t = self.table(tgt_table)
            # fix column names in df, and drop extra columns
            df = df[t.columns]
            # fix datatypes in df
            df = _schema_apply_to(t.schema(), df)

        df.to_csv(tmp_file, index=False, header=False)

        res = self.copy_from(tgt_table, tmp_file, header=False, max_reject=0)

        os.remove(tmp_file)
        self.log(
            "store_by_copy", tstart, tgt_table, sources=sources, rows_input=len(df)
        )
        return res

    def copy_to(self, table_name, from_expr, to_filename, **kwargs):
        props = self._with_props(kwargs)
        q = f"""COPY ( {self.to_sql(from_expr)} ) TO '{to_filename}' {props};"""
        return self.exec_update(None, q, sources=[table_name], cmd="COPY TO")

    def copy_to_and_from(
        self, src_table, from_expr, tgt_table, ddl=None, drop=False, **kwargs
    ):
        """
        params: used to label the file name
        """
        tstart = time()
        fname = self.clean_name(
            "__".join(
                [
                    "copy_to_and_from",
                    src_table,
                    tgt_table,
                    datetime.datetime.now().isoformat(),
                ]
            )
        )
        tmp_file = f"{self._shared_tmp_dir()}/{fname}.csv"
        if os.path.exists(tmp_file):
            os.remove(tmp_file)
        if drop:
            self.drop_table(tgt_table)
        self.table(src_table)

        self.copy_to(src_table, from_expr, tmp_file, header=False, **kwargs)

        if ddl and not self.exists_table(tgt_table):
            self.create_table(tgt_table, ddl)

        res = self.copy_from(tgt_table, tmp_file, header=False, max_reject=0, **kwargs)

        os.remove(tmp_file)
        self.log("copy_to_and_from", tstart, tgt_table)
        return res

    def _store_expr(
        self,
        table_name,
        expr,
        skip_if_exists=False,
        take_counts=True,
        ddl=None,
        fragment_size=None,
    ):
        tstart = time()

        before = 0

        if not self.exists_table(table_name) and ddl:
            self.create_table(table_name, ddl)

        if self.exists_table(table_name):

            if skip_if_exists:
                return table_name
            else:
                if take_counts:
                    t = self.table(table_name)
                    before = self.con.execute(t.count())

                self.insert_as(table_name, expr)

        else:
            self.create_table_as(table_name, expr, fragment_size=fragment_size)

        tend = time()

        after = None
        if take_counts:
            t = self.table(table_name)
            after = self.con.execute(t.count())

        self.log("store_expr", tstart, table_name, None, before, after)

        return table_name

    def _load_table_from_df(self, table_name, ddl, df, take_counts=True):
        sources = list(set(self._names(self.sources)))
        logi(
            cmd="load_table_from_df",
            sources=sources,
            target=table_name,
            process_rows=len(df),
        )

        if len(df) == 0:
            return table_name

        tstart = time()

        before = 0
        if self.exists_table(table_name):
            if take_counts:
                t = self.table(table_name)
                before = self.con.execute(t.count())

        elif ddl:
            self.create_table(table_name, ddl)

        if self.dryrun:
            print(f"-- load_table {table_name} rows=" + len(df))
        else:
            if self.exists_table(table_name):
                t = self.table(table_name)
                # fix column names in df, and drop extra columns
                df = df[t.columns]
                # fix datatypes in df
                df = _schema_apply_to(t.schema(), df)

            self.con.load_data(table_name, df)

        tend = time()

        rejected = None
        after = None
        if take_counts:
            t = self.table(table_name)
            after = self.con.execute(t.count())
            rejected = len(df.index) - after + before

        msg = dict(
            cmd="load_table_from_df",
            time_s=round(tend - tstart, 2),
            target=table_name,
            ct_input=len(df.index),
            ct_before=before,
            ct_after=after,
            rejected=rejected,
            sources=list(set(self._names(self.sources))),
        )
        if take_counts and rejected is not None and rejected > 0:
            # this would only make sense if no parallel process is changing the same table
            raise Exception("Records rejected. " + str(msg))
        else:
            self.log(
                "load_table_from_df",
                tstart,
                table_name,
                None,
                before,
                after,
                process_rows=len(df.index),
                rejected=rejected,
            )

        return table_name

    def load_table(
        self,
        table_name,
        expr,
        ddl=None,
        sources=None,
        drop=False,
        is_temporary=False,
        skip_if_exists=False,
        take_counts=True,
        fragment_size=None,
    ):
        if sources:
            logi(
                cmd="load_table",
                sources=list(set(self._names(sources))),
                target=table_name,
            )
        if drop:
            self.drop_table(table_name)
        if isinstance(expr, pd.DataFrame):
            return self._load_table_from_df(
                table_name, ddl=ddl, df=expr, take_counts=take_counts
            )
        else:
            return self._store_expr(
                table_name,
                expr,
                skip_if_exists=skip_if_exists,
                ddl=ddl,
                take_counts=take_counts,
                fragment_size=fragment_size,
            )

    def store(
        self,
        data,
        load_table,
        ddl=None,
        drop=False,
        is_temporary=False,
        skip_if_exists=False,
        take_counts=True,
        fragment_size=None,
        sources=None,
    ):
        """
        Loads `data` into a table if load_table is not None.
        Logs the sources from the table names from when `table()` was invoked.
        data: an Ibis expr or Pandas DF.
        ddl: should be provided if data is a DF and the table might not exist.
        Returns: the load_table name if the data was stored in a table, or the data.
        """
        if load_table:
            return self.load_table(
                load_table,
                data,
                ddl=ddl,
                sources=sources,
                drop=drop,
                is_temporary=is_temporary,
                skip_if_exists=skip_if_exists,
                take_counts=take_counts,
                fragment_size=fragment_size,
            )
        else:
            return data


def col_renames(appendage, *cols):
    return [col.name(col.get_name() + appendage) for col in cols]


def connect(con=None, close_on_exit=True):
    """
    Connect to OmniSciDB.
    For use with Prefect, though does not depend on the Prefect API itself (other than logging).

    Typical usage will be with a Prefect task function.
    For example:

    @task
    def ctas_something(con, input_table_name, load_table=None):
        # The con in the function parameter may actually be a URL
        # Shadow the con variable name to avoid misuse.
        with omnisci_olio.workfow.connect(con) as con:

            # Referencing a table will be logged as a source of the store/load_table operation
            input_table = con.table(input_table_name)

            # build an ibis expression from one or more input tables
            expr = input_table.count()

            # The con can be used in another with block, it will not be closed by that block.

        # Depending on the value of load_table:
        # if load_table is a name , the CTAS will be executed and the name returned
        # if None, the expr will be returned, which is useful for testing in a notebook.
        # The expr can be an Ibis expression or Pandas DataFrame.
        return con.store(expr, load_table, drop=True, is_temporary=False)

    Args:
        - con (URL string, Ibis connection, OmniSciDBClient, or None to use env var OMNISCI_DB_URL)
        - close_on_exit (bool, default True): if False, don't automatically close the connection in a nested `with` block so the parent block can continue.

    Returns:
        OmniSciDBClient: with a Ibis con and Pyomnisci con.con, connected to OmniSciDB
    """
    if con is None or isinstance(con, str):
        return OmniSciDBClient(uri=con, close_on_exit=close_on_exit)
    elif isinstance(con, OmniSciDBClient):
        # return con
        return OmniSciDBClient(_other=con, close_on_exit=False)
    elif isinstance(con, OmniSciDBBackend):
        return OmniSciDBClient(con=con, close_on_exit=False)
    else:
        raise Exception("Unrecognized type: %s" % type(con))
