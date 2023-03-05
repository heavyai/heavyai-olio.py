import os
import re
import json
from base64 import b64decode, b64encode
from datetime import datetime
from heavydb.thrift.ttypes import TDashboard


def _objwalk(key, obj, func1):
    if isinstance(obj, dict):
        ret = {}
        for key, value in obj.items():
            # ret[ func1(key) ] = _objwalk(value, func1)
            k, v = func1(func1(None, key), _objwalk(key, value, func1))
            ret[k] = v
        return ret
    elif isinstance(obj, list) and not isinstance(obj, str):
        return [_objwalk(None, value, func1) for value in obj]
    return func1(None, obj)


def _source_remapper(remap):
    # sort to use the longest table name first, in case one is a substring of another
    remap_keys = list(remap.keys())
    remap_keys.sort(key=len, reverse=True)

    def source_remap_func1(key, value):
        if isinstance(value, str):
            if value in remap_keys:
                if key is None:
                    # print('a', value)
                    return remap[value]["name"]
                else:
                    # print('b', key, value)
                    return key, remap[value]["name"]
            for k in remap_keys:
                name = remap[k]["name"]
                m = re.match(f'([ ,"]|^){k}([ ,\."]|$)', value)
                if m:
                    # print('c', key, value)
                    value = value.replace(k, name)
                if key is None:
                    return value
                else:
                    return key, value
        return value if key is None else (key, value)

    return source_remap_func1


def _change_dashboard_sources(dashboard, remap):
    """
    Remap a dashboard to use a new table
    Parameters
    ----------
    dashboard: A dictionary containing the old dashboard state
    remap: A dictionary containing the new dashboard state to be mapped
    Returns
    -------
    dashboard: A base64 encoded json object containing the new dashboard state
    """
    dm = json.loads(dashboard.dashboard_metadata)
    tlst = map(str.strip, dm.get("table", "").split(","))
    tlst = [remap[t]["name"] if remap.get(t, {}).get("name", {}) else t for t in tlst]
    dm["table"] = ", ".join(tlst)

    # Load our dashboard state into a python dictionary
    ds = json.loads(b64decode(dashboard.dashboard_state).decode("utf-8"))

    # with open('src.json', 'w') as f:
    #     json.dump(ds, f, indent=4, sort_keys=True)

    ds = _objwalk(None, ds, _source_remapper(remap))

    # with open('_objwalk.json', 'w') as f:
    #     json.dump(ds, f, indent=4, sort_keys=True)

    # Convert our new dashboard state to a python object
    dashboard.dashboard_state = b64encode(json.dumps(ds).encode()).decode()
    dashboard.dashboard_metadata = json.dumps(dm)
    return dashboard


def duplicate_dashboard(con, dashboard_id, new_name=None, source_remap=None):
    """
    Duplicate an existing dashboard, returning the new dashboard id.

    Parameters
    ----------

    dashboard_id: int
        The id of the dashboard to duplicate
    new_name: str
        The name for the new dashboard
    source_remap: dict
        EXPERIMENTAL
        A dictionary remapping table names. The old table name(s)
        should be keys of the dict, with each value being another
        dict with a 'name' key holding the new table value. This
        structure can be used later to support changing column
        names.

    Examples
    --------
    >>> source_remap = {'oldtablename1': {'name': 'newtablename1'}}
    >>> newdash = con.duplicate_dashboard(12345, "new dash", source_remap)
    """

    source_remap = source_remap or {}
    d = con._client.get_dashboard(session=con._session, dashboard_id=dashboard_id)

    newdashname = new_name or "{0} (Copy {1})".format(
        d.dashboard_name, datetime.now().isoformat()
    )
    d = _change_dashboard_sources(d, source_remap) if source_remap else d
    d.dashboard_name = newdashname

    return con.create_dashboard(d)


def dashboards_remap_tables(
    src_con,
    tgt_con,
    source_remap=None,
    replace=False,
):
    results = []
    for id in src_con.con.get_dashboards():
        src = src_con.con._client.get_dashboard(
            session=src_con.con._session, dashboard_id=id.dashboard_id
        )
        src_dashboard_state = src.dashboard_state
        src_dashboard_metadata = src.dashboard_metadata

        tgt = _change_dashboard_sources(src, source_remap) if source_remap else src
        tgt.dashboard_name = src.dashboard_name

        if (src_dashboard_state != tgt.dashboard_state) or (
            src_dashboard_metadata != tgt.dashboard_metadata
        ):
            if replace:
                tgt_con.con._client.replace_dashboard(
                    session=tgt_con.con._session,
                    dashboard_id=src.dashboard_id,
                    dashboard_name=tgt.dashboard_name,
                    dashboard_owner=tgt.dashboard_owner,
                    dashboard_state=tgt.dashboard_state,
                    image_hash=None,
                    dashboard_metadata=tgt.dashboard_metadata,
                )
                results.append(src.dashboard_id)
            else:
                id = tgt_con.con.create_dashboard(tgt)
                results.append(id)

    return results
