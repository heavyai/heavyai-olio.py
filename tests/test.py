import os
import sys

from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.abspath(__file__)) + '/..')

import heavyai_olio.workflow as hw
import heavyai_olio.schema as sc

load_dotenv(dotenv_path=os.path.dirname(os.path.abspath(__file__)) + '/../project.env', override=True, verbose=True)
load_dotenv(dotenv_path=os.path.dirname(os.path.abspath(__file__)) + '/../.env', override=True, verbose=True)

def main(args: list) -> None:
    client: hw.client.HeavyDBClient = hw.connect()

if __name__ == '__main__':
    main(sys.argv[1:])
