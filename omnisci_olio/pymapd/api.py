#
# TODO consider moving some of these functions to pymapd
#

import re
import pandas as pd


def select(con, operation, parameters=None,
            first_n: int = -1, ipc: bool = None, gpu_device: int = None):
    """
    Executes the SQL operation, delegates to ``execute``, ``select_ipc`` or ``select_ipc_gpu``
    depending on the value of ``ipc`` and ``gpu_device``.

    Parameters
    ----------
    operation: str
        A SQL statement
    parameters: dict, optional
        Parameters to insert into a parametrized query
    first_n: int, optional
        Number of records to return
    ipc: bool, optional, default ``None``
        Enable Inter Process Communication (IPC) execution type.
        ``ipc`` default value when ``gpu_device`` is None is False (same as ``select_ipc``),
        otherwise its default value is True (same as ``select_ipc_gpu``).
    gpu_device: int, optional, default ``None``
        GPU device ID.
    
    Returns
    -------
    output: execution type dependent
        If IPC and with no GPU: ``pandas.DataFrame``
        If IPC and GPU: ``cudf.DataFrame``
        If not IPC and with no GPU: ``pandas.DataFrame``
    """
    if ipc in (None, False) and gpu_device is None:
        return pd.read_sql(operation, con)
        # cursor = con.execute(operation)
        # return pd.DataFrame(
        #     cursor.fetchmany(first_n) if first_n >= 0 else cursor.fetchall(),
        #     columns=[d.name for d in cursor.description])
    elif ipc and gpu_device is None:
        return con.select_ipc(operation, parameters, first_n)
    elif gpu_device is not None:
        return con.select_ipc_gpu(operation, parameters, gpu_device, first_n)


def status(con):
    s = con._client.get_status(con.sessionid)
    return pd.DataFrame([t.__dict__ for t in s])


def session(con):
    s = con._client.get_session_info(con.sessionid)
    return pd.DataFrame([s.__dict__])


def copy_from(con, copy_from_sql):
    """
    To be used with a 'COPY FROM' statement.
    Calls pymapd `execute`, but parses the response to check if it failed or not.
    If it fails to do too many rejected records, raise an exception.
    If it succeeds, return a dict with loaded, rejected, and time in ms.
    """
    rs = con.execute(copy_from_sql).fetchall()
    msg = rs[0][0]
    if msg.startswith('Loaded:'):
        m = re.match('Loaded: ([0-9]+) recs, Rejected: ([0-9]+) recs in ([0-9.]+) secs', msg)
        if m:
            return {'loaded': m[1], 'rejected': m[2], 'time': m[3]}
        else:
            # never seen this, but just in case
            return {'message': msg}
    elif msg.startswith('Creating ') or msg.startswith('Appending '):
        # with geo=true is not very descriptive
        return {'message': msg}
    else:
        raise Exception(str(rs))


def table_details(con, table_name):
    return pd.DataFrame(con.get_table_details(table_name))


def clear_cpu_memory(con):
    return con._client.clear_cpu_memory(con._session)


def clear_gpu_memory(con):
    return con._client.clear_gpu_memory(con._session)
