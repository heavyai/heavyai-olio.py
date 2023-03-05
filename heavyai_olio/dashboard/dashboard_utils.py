import os
import logging
from glob import glob
import pandas as pd
from base64 import b64decode
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import json
from heavydb.thrift.ttypes import TDashboard
from ibis_heavyai import Backend
from .dashboard_export import *
from .dashboard_data import dashboard_charts, dashboard_chart_projections
from .dashboard_edit import _objwalk, _source_remapper

# module level logger which extends root logger
L = logging.getLogger(__name__)

@dataclass
class DashboardUtils:
    """
    Set of dashboard utility functions.
    """

    backend: Backend

    def get_dashboards(self) -> Dict[str, TDashboard]:
        """
        Get all heavyai dashboards created through immerse.
        """
        L.info('Getting dashboards...')
        boards = dict()
        for board in self.backend.con.get_dashboards():
            boards[board.dashboard_name.strip()] = board
        return boards

    def export_dashboard(self, id, dashboards_dir="dashboards") -> str:
        """
        Export dashboard to a json file.
        """
        src = self.backend.con._client.get_dashboard(session=self.backend.con._session, dashboard_id=id)

        filename = os.path.join(dashboards_dir, src.dashboard_name.strip() + ".json")

        with open(filename, "w") as out:
            try:
                metadata = json.loads(src.dashboard_metadata)
            except:
                metadata = (src.dashboard_metadata.strip(),)
            obj = dict(
                dashboard_id=src.dashboard_id,
                name=src.dashboard_name.strip(),
                metadata=metadata,
                # image_hash = src.image_hash,
                last_update_time=src.update_time,
                owner=src.dashboard_owner,
                is_dash_shared=src.is_dash_shared,
                permissions=src.dashboard_permissions.__dict__,
                state=json.loads(b64decode(src.dashboard_state).decode("utf-8")),
            )
            json.dump(obj, out, sort_keys=True, indent=4)
            L.info(f'Dashboard {src.dashboard_name} sucessfully exported to {filename}')
            return filename

    def export_dashboards(self, dashboards_dir="dashboards", delete_files=False) -> List[str]:
        """
        Export dashboards to a directory.
        Existing json files will be deleted.
        It is expected the dir is under source control (e.g. git), but that is not managed by this function.
        """
        os.makedirs(dashboards_dir, exist_ok=True)
        if delete_files:
            for filename in glob(f"{dashboards_dir}/*.json"):
                os.remove(filename)
                L.info(f'Dashboard file {filename} got deleted.')

        return [self.export_dashboard(board.dashboard_id, dashboards_dir=dashboards_dir)
                for board in self.backend.con.get_dashboards()]

    def import_dashboards(self, dashboards_dir='dashboards') -> List[int]:
        """
        Import dashboard files (from export_dashboards) to Immerse.
        """
        L.info('Bulk importing dashboards...')
        boards = self.get_dashboards()

        return [
            self.sync_dashboard(filename=filename, boards=boards)
            for filename in glob(f"{dashboards_dir}/*.json")
        ]

    def import_key_dashboards(self, dashboards, dashboards_dir="dashboards", replace=True):
        """
        Import only the dasboards which are exists in the passed dashboards list.
        """
        key_dashboards_dict = {board.name: board for board in dashboards}

        boards = self.get_dashboards()

        for filename in glob(f"{dashboards_dir}/*.json"):
            board = read_dashboard(filename)
            if board['name'] in key_dashboards_dict:
                if replace or board['name'] not in boards:
                    self.sync_dashboard(dashboard=board, boards=boards)

    def read_dashboard(self, filename) -> Dict[str, Any]:
        """
        Read the contents of exported dashboard file.

        Args:
            filename ([str]): path to dashboard json file.
        """
        try:
            L.info(f'Reading dashboard file {filename}')
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

    @staticmethod
    def diff_dashboards(a, b) -> List[Tuple[str, Any, Any]]:
        """
        Find difference between two dashboard datas.
        """
        ats = pd.to_datetime(a.update_time)
        bts = pd.to_datetime(b.update_time)
        if ats > bts:
            L.info(f'diff_dashboards: target dashboard is updated more recently {ats}, {bts}, {a.dashboard_id}, {a.dashboard_name}')
            return None

        a = a.__dict__
        b = b.__dict__

        diff = [
            (key, a[key], b[key])
            for key in ["dashboard_id", "dashboard_name", "dashboard_metadata", "dashboard_state"]
            if a[key] != b[key]
        ]

        if diff:
            L.info(f'diff {diff}')

        return diff

    def create_dashboard(self, td: TDashboard) -> int:
        """
        Create new dashboard.
        """
        id = self.backend.con.create_dashboard(td)
        L.info(f"created_dashboard {id}, {td.dashboard_name}")
        return id

    def replace_dashboard(self, td: TDashboard) -> int:
        """
        Replace existsing dashboard with new data.
        """
        self.backend.con._client.replace_dashboard(
            session = self.backend.con._session,
            dashboard_id = td.dashboard_id,
            dashboard_name = td.dashboard_name,
            dashboard_owner = td.dashboard_owner,
            dashboard_state = td.dashboard_state,
            image_hash = td.image_hash,
            dashboard_metadata = td.dashboard_metadata,
        )
        L.info(f"replace_dashboard {td.dashboard_id}, {td.dashboard_name}")
        return td.dashboard_id

    def sync_dashboard(self, filename: Optional[str]=None, dashboard: Optional[Dict[str, Any]]=None, boards: List[Any]=None) -> int:
        """
        Sync given dashboard with the given or existing boards.

        Args:
            filename (Optional[str], optional): dashboard filepath to sync.. Defaults to None.
            dashboard (Optional[Dict[str, Any]], optional): dashboard dict to sync. Defaults to None.
            boards (List[Any], optional): List of dashboards to check with. Defaults to None.
        """
        board = dashboard or self.read_dashboard(filename)
        boards = boards or self.get_dashboards()
        
        L.info(f'Syncing dashboard {board.id} with the passed or existing boards.')

        try:
            td = TDashboard()
            td.dashboard_name = board['name']
            td.dashboard_metadata = json.dumps(board['metadata'])
            td.dashboard_state = b64encode(json.dumps(board['state']).encode()).decode()
            td.dashboard_owner = board.get('owner')
            td.update_time = board.get('last_update_time')
            if board['name'] not in boards:
                L.info('Given Dashboard not exists on the existing boards, so creating new')
                return self.create_dashboard(td)

            # board already exists
            # get the old board from database
            old_board = boards[board['name']]
            old_board = self.backend.con._client.get_dashboard(session=self.backend.con._session, dashboard_id=old_board.dashboard_id)
            td.dashboard_id = old_board.dashboard_id
            # find diff between old board and the board that we want to sync
            # if both are same then we don't need to do anything
            if not self.diff_dashboards(old_board, td):
                # no differences found
                # so return the board id as it is
                return td.dashboard_id
            # differences found, so replace the db board with the new board
            L.debug('Going to replace dashbord.')
            return self.replace_dashboard(td)

        except Exception as e:
            raise Exception(filename) from e

    def get_dashboard_dict(self, dashboard_id: int) -> Dict[str, Any]:
        """
        Get dashboard from dashboard id, build and return it as dict.
        """
        board = self.backend.con.get_dashboard(dashboard_id)
        result = dict(
            name=board.dashboard_name,
            metadata=json.loads(board.dashboard_metadata),
            state=json.loads(b64decode(board.dashboard_state).decode("utf-8")),
        )
        return result

    def get_dashboard_json(self, dashboard_id: int) -> Dict[Any, Any]:
        """
        Get dashboard state as json dict.
        """
        return self.get_dashboard_dict(dashboard_id).get('state', {})

    def get_dasboards_projections(self, hostname=None, database=None) -> pd.DataFrame:
        """
        Get dashboards projections.
        """
        L.info('Getting dashboards projections.')

        r = []
        for d in self.backend.con.get_dashboards():
            try:
                dash = self.get_dashboard_json(d.dashboard_id)
                if "tabs" in dash:
                    for tab in dash["tabs"].values():
                        c = dashboard_charts(tab)
                        if c is not None:
                            c["dashboard_tab_id"] = tab.get("tabId")
                            c["dashboard_tab_name"] = tab.get("tabName")
                            c = dashboard_chart_projections(c)
                            r.append(c)
                else:
                    c = dashboard_charts(dash)
                    if c is not None:
                        c = dashboard_chart_projections(c)
                        r.append(c)
            except Exception as e:
                L.warning(f'get_dasboards_projections: exception occurs: {e}')
                row = {}
                row["dashboard_id"] = d.dashboard_id
                # row['dashboard_title'] = d.dashboard_title
                #             row['dashboard_table'] = dash.get('table')
                #             row['dashboard_version'] = dash.get('version')
                #             row['dashboard_owner'] = dash.get('owner')
                row["error"] = str(e)
                r.append(pd.DataFrame([row]))

        df = pd.concat(r)
        df["hostname"] = hostname if hostname else self.backend.host
        df["db"] = (
            database
            if database
            else self.backend.con._client.get_session_info(self.backend.con._session).database
        )
        L.info(f'projections df: \n{df.to_string()}')
        return df

    def get_dashboards_dataframe(self) -> pd.DataFrame:
        """
        Form pd.dataframe from list of dashboards.
        """
        return pd.DataFrame([x.__dict__ for x in self.backend.con.get_dashboards()])

    def get_dashboards_tabs(self) -> pd.DataFrame:
        """
        Form df from dashboard tabs.
        """
        L.info('Getting dashboards tabs dataframe.')
        r = []
        for board_t in self.backend.con.get_dashboards():
            board = self.get_dashboard_json(board_t.dashboard_id)
            if "tabs" in board:
                for tab in board["tabs"].values():
                    r.append(tab)
            else:
                r.append(board)
        df = pd.DataFrame(
            [
                {
                    **board_t.__dict__,
                    **{"tabId": tab.get("tabId"), "tabName": tab.get("tabName")},
                }
                for tab in r
            ]
        )
        L.info(f'dashboards tabs df: \n{df.to_string()}')
        return df

    @staticmethod
    def change_dashboard_sources(dashboard: TDashboard, remap: Dict[Any, Any]) -> TDashboard:
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

    def duplicate_dashboard(self, dashboard_id, new_name=None, source_remap=None) -> int:
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
        >>> dash_utils = DashboardUtils(backend=backend)
        >>> newdash_id = dash_utils.duplicate_dashboard(12345, "new dash", source_remap)
        """

        source_remap = source_remap or {}
        dashboard = self.backend.con.get_dashboard(dashboard_id=dashboard_id)

        newdashname = new_name or "{0} (Copy {1})".format(
            dashboard.dashboard_name, datetime.now().isoformat()
        )
        d = self.change_dashboard_sources(dashboard, source_remap) if source_remap else dashboard
        d.dashboard_name = newdashname

        return self.create_dashboard(d)

    @staticmethod
    def dashboards_remap_tables(src_con: Backend, tgt_con: Backend, source_remap=None, replace=False) -> List[int]:
        results = []
        for id in src_con.con.get_dashboards():
            src = src_con.con._client.get_dashboard(
                session=src_con.con._session, dashboard_id=id.dashboard_id
            )
            src_dashboard_state = src.dashboard_state
            src_dashboard_metadata = src.dashboard_metadata

            tgt = DashboardUtils.change_dashboard_sources(src, source_remap) if source_remap else src
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
