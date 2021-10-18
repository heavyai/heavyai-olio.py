import os
import re
import time
from math import log10, floor
from functools import reduce
import csv
import pandas as pd
from collections import namedtuple
from datetime import datetime
from types import SimpleNamespace


def round_sig(x, sig=2):
    return round(x, sig - int(floor(log10(abs(x)))) - 1)


def checksum(st):
    return reduce(lambda x, y: x + y, map(ord, st))


# TODO the regex are super-slow for very long SQL and/or VEGA

# !grep -n 'stdlog sql_execute' $logfile | head -1
# re_sql = re.compile(r"""sql_execute [0-9]+ ([0-9]+) (.*) .* .* {"query_str","execution_time_ms","total_time_ms"} {"(.*)","[0-9]+","[0-9]+"}""", re.MULTILINE|re.DOTALL)

# 2019-11-11T19:06:39.325374 1 16 MapDHandler.cpp:946 stdlog_begin sql_execute 25 0 mapd mike.hinchey 628-5DjJ {"query_str"} {"SELECT COUNT(*) AS n FROM zendesk_jira"}
re_sql = re.compile(
    r"""sql_execute [0-9]+ ([0-9]+) (.*) .* .* {"query_str"} {"(.*)"}""",
    re.MULTILINE | re.DOTALL,
)


def parse_sql(line, event):
    # print('\nLINE:', line, 'LINE_END')
    match = re_sql.match(line)
    # print(line, match)
    if match:
        event.func = "sql_execute"
        event.dur_ms = int(match.group(1))
        event.dbname = match.group(2)
        event.query = match.group(3).strip()
        event.query = event.query.replace('""', '"')
        event.query_checksum = checksum(event.query)

        parts = event.query.split(" ", 1)
        cmd = parts[0].upper()
        if cmd in ["SELECT", "WITH", "EXPLAIN"]:
            event.modify = False

        else:  # elif cmd in ['COPY', 'CREATE', 'DROP', 'TRUNCATE', 'UPDATE', 'INSERT', 'DELETE', 'ALTER', 'GRANT', 'REVOKE', 'OPTIMIZE']:
            event.modify = True
        return event


def exec_sql(event):
    # TODO connect event.dbname

    tstart = time.time()
    c = con.con.execute(query)
    tend = time.time()
    event.dur_ms = tend - tstart
    tsec = round_sig(1000 * (tend - tstart), 1)
    rs = c.fetchall()
    count = len(rs)
    # output.write(f"""|metrics time={tsec:n} count={count}\n""")


# !grep -n 'stdlog render_vega' $logfile | head -1
re_vega = re.compile(
    r"""render_vega [0-9]+ ([0-9]+) (.*) .* .* {"widget_id","compression_level","vega_json","nonce"} {"[0-9]+","[0-9]+","(.*)","[0-9]+"}""",
    re.MULTILINE | re.DOTALL,
)


def parse_vega(line, event):
    match = re_vega.match(line)
    if match:
        event.func = "render_vega"
        event.dur_ms = int(match.group(1))
        event.dbname = match.group(2)
        event.query = match.group(3).strip()
        event.query = event.query.replace('""', '"')
        event.query_checksum = checksum(event.query)
        return event


def exec_vega(event):
    # TODO connect event.dbname

    tstart = time.time()
    png = con.con.render_vega(vega)
    tend = time.time()
    event.dur_ms = tend - tstart
    tsec = round_sig(1000 * (tend - tstart), 1)
    return event


def transform_lines(line, event):
    pos = line.find(" stdlog ")
    z = 8
    if pos == -1:
        pos = line.find(" stdlog_begin ")
        z = 14
    if pos >= 0:
        line = line[pos + z :]
        if line.startswith("sql_execute "):
            return parse_sql(line, event)
        elif line.startswith("render_vega "):
            return parse_vega(line, event)


def read_log_file_sql(
    log_path, load_label, quit_on_error=True, max_queries=1000000, max_lines=20000000
):

    run_tstamp = None

    beginning = r""" . [0-9]+ [A-Za-z\.\:0-9]+ """

    with open(log_path, "r") as input:
        events = []
        ctlines = 0
        ct_events = 0
        prev_line = None
        lines = []
        event = SimpleNamespace()
        for line in input:
            if ctlines >= max_lines:
                print("max_lines", ctlines)
                break
            ctlines += 1
            if ct_events >= max_queries:
                print("max_queries", ct_events)
                break

            if len(lines) == 1 and (
                line.find("sql_execute") == -1 and line.find("render_vega") == -1
            ):
                # print('continue', line)
                continue
            # else:
            #     print('keep', len(lines), line)

            try:
                try:
                    tstamp = pd.to_datetime(line[:26])
                except ValueError as e:
                    tstamp = None
                match = re.match(beginning, line[26:])
                if tstamp and match:
                    if run_tstamp is None:
                        run_tstamp = tstamp
                    event.tstamp = tstamp
                    event.run_tstamp = run_tstamp

                    event = transform_lines("".join(lines), event)
                    if event:
                        # print(event)
                        events.append(event)
                        ct_events += 1

                    lines = [line]
                    event = SimpleNamespace()
                else:
                    # print('NO MATCH', line)
                    lines.append(line)

            except Exception as e:
                ct_events += 1
                if quit_on_error:
                    raise e
                else:
                    print(e)
        event = transform_lines("".join(lines), event)
        if event:
            events.append(event)
    len(events)

    df = pd.DataFrame([x.__dict__ for x in events])
    df["load_label"] = load_label
    df["load_timestamp"] = datetime.now()
    df["srcfile"] = log_path
    return df


# def read_log_files_sql(log_paths, load_label, quit_on_error = True, max_lines = 20000000):
#     return pd.concat([read_log_file_sql(p, load_label, quit_on_error, max_lines) for p in log_paths])
