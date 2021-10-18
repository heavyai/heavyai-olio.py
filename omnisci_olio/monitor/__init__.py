"""OmniSci system resource and DB monitor"""

import sys
from .monitor import monitor_import


def main(argv):
    monitor_import(int(argv[1]), int(argv[2]), argv[3] if len(argv) > 3 else None)


if __name__ == "__main__":
    main(sys.argv[1:])
