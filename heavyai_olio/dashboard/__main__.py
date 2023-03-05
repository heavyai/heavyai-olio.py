import sys, os
from pathlib import Path
import argparse


path = str(Path(__file__).parent.parent.parent.absolute())
sys.path.append(path)

import heavyai_olio.ibis
from heavyai_olio.dashboard import *
from heavyai_olio.dashboard.dashboard_utils import DashboardUtils


def main(args: argparse.Namespace):
    backend = heavyai_olio.ibis.connect() # create ibis backend at the start and finally close it at end
    dash_utils = DashboardUtils(backend=backend)

    if args.get_dashboards:
        print(dash_utils.get_dashboards())
    elif args.export_dashboards:
        print(dash_utils.export_dashboards(dashboards_dir=args.export_dashboards))
    elif args.export_dashboard:
        args_len = len(args.export_dashboard)
        if args_len == 1:
            print(dash_utils.export_dashboard(args.export_dashboard[0]))
        elif args_len == 2:
            print(dash_utils.export_dashboard(args.export_dashboard[0], dashboards_dir=args.export_dashboard[1]))
    elif args.read_dashboard:
        print(dash_utils.read_dashboard(args.read_dashboard))
    elif args.sync_dashboard:
        print(dash_utils.sync_dashboard(filename=args.sync_dashboard))

    dash_utils.backend.close()


if __name__ == "__main__":
    """
    Set of dashboard operations which can be called directly.
    """

    parser = argparse.ArgumentParser(description='Process OmniSci dashboards.')
    parser.add_argument('--export-dashboard', nargs='*', action="store", help='Export dashboard as json file into target directory.')
    parser.add_argument('--export-dashboards', nargs='?', const='dashboards', type=str, default=None,
                        help='Export dashboards to target directory.')
    parser.add_argument('--get-dashboards', action='store_true')
    parser.add_argument('--read-dashboard', nargs='?', type=str, default=None, help='Read dashboard file.')
    parser.add_argument('--sync-dashboard', nargs='?', type=str, default=None, help='Sync dashboard file with existsing dashboards.')

    args = parser.parse_args()
    if args.export_dashboard is not None:
        if len(args.export_dashboard) not in [0, 1, 2]:
            parser.error('Either give no values for export_dashboard, or oen or two, not {}.'.format(len(args.export_dashboard)))
        try:
            args.export_dashboard[0] = int(args.export_dashboard[0])
        except ValueError:
            parser.error('Dashboard id should be an integer value.')

    main(args)
