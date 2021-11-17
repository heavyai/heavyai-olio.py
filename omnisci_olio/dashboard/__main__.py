import sys
import omnisci_olio.ibis
from omnisci_olio.dashboard import *


def main(argv):
    if len(argv) == 0:
        pass
    else:
        cmd = argv[0]
        if cmd == "export_dashboards":
            with omnisci_olio.ibis.connect() as con:
                export_dashboards(con, argv[1])
        if cmd == "import_dashboard":
            with omnisci_olio.ibis.connect() as con:
                import_dashboard(con, argv[1])


if __name__ == "__main__":
    main(sys.argv[1:])
