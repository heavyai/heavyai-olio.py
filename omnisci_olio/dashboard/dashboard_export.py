import os
import json
from glob import glob
from base64 import b64decode, b64encode
from datetime import datetime
import pandas as pd
from omnisci.thrift.ttypes import TDashboard


def export_dashboard(con, id, dashboards_dir="dashboards"):
    src = con.con._client.get_dashboard(session=con.con._session, dashboard_id=id)

    filename = os.path.join(dashboards_dir, src.dashboard_name.strip() + ".json")
    # print(src.dashboard_id, filename)

    with open(filename, "w") as out:
        try:
            metadata = json.loads(src.dashboard_metadata)
        except:
            metadata = src.dashboard_metadata.strip(),
        obj = dict(
            dashboard_id = src.dashboard_id,
            name = src.dashboard_name.strip(),
            metadata = metadata,
            # image_hash = src.image_hash,
            last_update_time = src.update_time,
            owner = src.dashboard_owner,
            is_dash_shared = src.is_dash_shared,
            permissions = src.dashboard_permissions.__dict__,
            state = json.loads(b64decode(src.dashboard_state).decode("utf-8")),
        )
        json.dump(obj, out, sort_keys=True, indent=4)
        return filename


def export_dashboards(con, dashboards_dir="dashboards", delete_files=False):
    """
    Export dashboards to a directory.
    Existing json files will be deleted.
    It is expected the dir is under source control (e.g. git), but that is not managed by this function.
    """
    os.makedirs(dashboards_dir, exist_ok=True)
    if delete_files:
        for filename in glob(f"{dashboards_dir}/*.json"):
            print("delete file", filename)
            os.remove(filename)

    return [export_dashboard(con, board.dashboard_id, dashboards_dir=dashboards_dir)
            for board in con.con.get_dashboards()]


def read_dashboard(filename):
    """
    Import dashboard file (from export_dashboards) to Immerse.
    """
    try:
        with open(filename) as fin:
            first_line = fin.readline()
            if first_line.startswith("{"):
                rest = fin.read()
                return json.loads(first_line + rest)
            else:
                td = dict()
                td['name'] = first_line.strip()
                td['metadata'] = fin.readline().strip()
                all = fin.read()
                td['state'] = json.loads(all)
                return td
    except Exception as e:
        raise Exception(filename) from e


def get_dashboards(con):
    boards = dict()
    for board in con.con.get_dashboards():
        boards[board.dashboard_name.strip()] = board
    return boards


def diff_dashboards(a, b):
    ats = pd.to_datetime(a.update_time)
    bts = pd.to_datetime(b.update_time)
    if ats > bts:
        print("diff_dashboards", "target dashboard is updated more recently", ats, bts, a.dashboard_id, a.dashboard_name)
        return None
    
    a = a.__dict__
    b = b.__dict__

    diff = [
        (key, a[key], b[key])
        for key in ["dashboard_id", "dashboard_name", "dashboard_metadata", "dashboard_state"]
        if a[key] != b[key]
    ]

    if diff:
        print("diff", diff)
    return diff


def sync_dashboard(con, filename=None, dashboard=None, boards=None):
    """
    Import dashboard file (from export_dashboards) to Immerse.
    """
    board = dashboard or read_dashboard(filename)
    boards = boards or get_dashboards(con)
    try:
        td = TDashboard()
        td.dashboard_name = board['name']
        td.dashboard_metadata = json.dumps(board['metadata'])
        td.dashboard_state = b64encode(json.dumps(board['state']).encode()).decode()
        td.dashboard_owner = board.get('owner')
        td.update_time = board.get('last_update_time')
        if board['name'] not in boards:
            id = con.con.create_dashboard(td)
            print("sync_dashboard", id, board['name'])
            return id
        else:
            old_board = boards[board['name']]
            old_board = con.con._client.get_dashboard(session=con.con._session, dashboard_id=old_board.dashboard_id)
            td.dashboard_id = old_board.dashboard_id
            if diff_dashboards(old_board, td):
                con.con._client.replace_dashboard(
                    session = con.con._session,
                    dashboard_id = td.dashboard_id,
                    dashboard_name = td.dashboard_name,
                    dashboard_owner = td.dashboard_owner,
                    dashboard_state = td.dashboard_state,
                    image_hash = td.image_hash,
                    dashboard_metadata = td.dashboard_metadata,
                )
                print("sync_dashboard", td.dashboard_id, td.dashboard_name)
                return td.dashboard_id
    except Exception as e:
        raise Exception(filename) from e


def import_dashboards(con, dashboards_dir):
    """
    Import dashboard files (from export_dashboards) to Immerse.
    """
    boards = get_dashboards(con)
    return [
        sync_dashboard(con, filename=filename, boards=boards)
        for filename in glob(f"{dashboards_dir}/*.json")
    ]


def import_key_dashboards(con, dashboards, dashboards_dir="dashboards", replace=True):
    key_dashboards_dict = {board.name: board for board in dashboards}

    boards = get_dashboards(con)

    for filename in glob(f"{dashboards_dir}/*.json"):
        board = read_dashboard(filename)
        if board['name'] in key_dashboards_dict:
            if replace or board['name'] not in boards:
                sync_dashboard(con, dashboard=board, boards=boards)


def dashboard_dict(con, dashboard_id):
    b = con.con._client.get_dashboard(
        session=con.con._session, dashboard_id=dashboard_id
    )
    r = dict(
        name=b.dashboard_name,
        metadata=json.loads(b.dashboard_metadata),
        state=json.loads(b64decode(b.dashboard_state).decode("utf-8")),
    )
    return r
