"""Runs forever in a loop, connects to HEAVY DB and calls thrift API to clear CPU and GPU memory"""

import sys
import time
import heavyai_olio.pymapd


def clear_memory_forever(sleep_seconds=3600):
    while True:
        with heavyai_olio.pymapd.connect() as con:
            print(heavyai_olio.pymapd.db_memory(con, detail=0).to_csv())
            heavyai_olio.pymapd.clear_cpu_memory(con)
            heavyai_olio.pymapd.clear_gpu_memory(con)
        time.sleep(sleep_seconds)


def main(argv):
    clear_memory_forever(int(argv[1]))


if __name__ == "__main__":
    main(sys.argv[1:])
