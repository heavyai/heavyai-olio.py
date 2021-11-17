import os
import subprocess
import sys
import pandas as pd
from sqlalchemy.engine.url import URL
from omnisci_olio.pymapd import url_prompt

try:
    from ibis_omniscidb import Backend as OmniSciDBBackend
except:
    # pre-2.0 ibis and ibis_omniscidb
    from ibis_omniscidb import OmniSciDBClient as OmniSciDBBackend


def omnisql(con, text):
    """
    Usage:
        omniscidb_util.omnisql(con, '\\status')
    """
    if isinstance(con, OmniSciDBBackend):
        if con.protocol is not None and con.protocol != "binary":
            protocol_arg = ["--" + con.protocol]
        else:
            protocol_arg = []
        si = con.con._client.get_session_info(con.con._session)
        params = [
            "omnisql",
            "-q",
            "--db",
            si.database,
            "-s",
            con.host,
            "--port",
            str(con.port),
            "-u",
            si.user,
            "-p",
            con.password,
        ] + protocol_arg
    else:
        if isinstance(con, URL):
            protocol = con.query.get("protocol")
        else:
            protocol = con.protocol
        if protocol is not None and protocol != "binary":
            protocol_arg = ["--" + protocol]
        else:
            protocol_arg = []

        params = [
            "omnisql",
            "-q",
            "--db",
            con.database,
            "-s",
            con.host,
            "--port",
            str(con.port),
            "-u",
            con.username,
            "-p",
            con.password,
        ] + protocol_arg
    p = subprocess.run(
        params,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        input=text,
        encoding="ascii",
    )
    if p.returncode != 0 or p.stderr:
        raise Exception("omnisql return_code={}: {}".format(p.returncode, p.stderr))
    # print(p)
    return p.stdout.strip().split("\n")


def ddl(con, table_name):
    # TODO rewrite to use table_details or sometime \d will be a thrift call
    return "\n".join(omnisql(con, "\\d " + table_name))


def cli_params(url=None):
    u = url_prompt(url)
    params = [
        "--db",
        url.database,
        "-s",
        url.host,
        "--port",
        str(url.port),
        "-u",
        url.username,
        "-p",
        url.password,
    ]
    print(" ".join(params))


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "cli_params":
        cli_params(sys.argv.get(2))
