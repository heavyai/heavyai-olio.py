import os
import logging
from glob import glob
from base64 import b64decode
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import json
from omnisci.thrift.ttypes import TDashboard
from ibis_omniscidb import Backend
from .dashboard_export import *

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
        Get all omnisci dashboards created through immerse.
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
    def diff_dashboards(a, b):
        return diff_dashboards(a, b)

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
