import os
import json
from time import time
import datetime

import pandas as pd
import pyomnisci
from .client import OmniSciDBClient, log_info, log_warning

OMNISCI_DB_URL = os.environ['OMNISCI_DB_URL']
OMNISCI_DB_TEST_URL = os.environ['OMNISCI_DB_TEST_URL']

def round_sig(x):
    return float("%.3g" % x)
    # return x if abs(x) < 100 else int(x)

class OmniSciDBTestClient (OmniSciDBClient):

    def __init__(self, url=None, switch_db=None):
        super().__init__(
            url or OMNISCI_DB_URL,
            log_uri = url or OMNISCI_DB_URL)
        if switch_db:
            self.con.con._client.switch_database(self.con.con._session, switch_db)

    def db_version(self):
        return self.con.con._client.get_server_status(self.con.con._session).version

    def get_database(self):
        return self.con.con._client.get_session_info(self.con.con._session).database

    def exec_time(self, q):
        try:
            tstart = time()
            res = self.con.con.execute(q).fetchall()
            return (res, time() - tstart)
        except Exception as e:
            raise Exception(q) from e


def connect(url=None, switch_db=None):
    return OmnisciTestClient(url, switch_db)

def count_table(con, tn):
    (res, t) = exec(con, f"SELECT COUNT(*) ct FROM {tn}")
    return (res[0], t)
