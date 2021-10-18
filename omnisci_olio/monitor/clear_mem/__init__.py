"""Runs forever in a loop, connects to OmniSci DB and calls thrift API to clear CPU and GPU memory"""

import sys
import omnisci_olio.pymapd


def clear_memory_forever(sleep_seconds=3600):
    while True:
        with omnisci_olio.pymapd.connect() as con:
            print(omnisci_olio.pymapd.db_memory(con, detail=0).to_csv())
            omnisci_olio.pymapd.clear_cpu_memory(con)
            omnisci_olio.pymapd.clear_gpu_memory(con)
        sleep(sleep_seconds)


def main(argv):
    clear_memory_forever(int(argv[1]))


if __name__ == "__main__":
    main(sys.argv[1:])
