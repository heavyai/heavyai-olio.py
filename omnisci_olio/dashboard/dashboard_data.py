import os
import re
import pandas as pd
import omnisci_olio.pymapd
import logging
import base64
import json

log = logging.getLogger("omnisci_schema")


# from estimatesize.sh
# searchterms = ["TEXT ENCODING", "TIMESTAMP", "TIME", "DATE", "FLOAT", "DOUBLE", "INTEGER", "SMALLINT", "BIGINT", "BOOLEAN", "DECIMAL"]
# datatypesizes=[8, 8, 8, 8, 4, 8, 4, 2, 8, 1, 8]
# {x[0]: x[1] for x in zip(searchterms, datatypesizes)}
dtype_sizes = {
    "STR": 8,
    "TIMESTAMP": 8,
    "TIME": 8,
    "DATE": 8,
    "FLOAT": 4,
    "DOUBLE": 8,
    "INT": 4,
    "SMALLINT": 2,
    "TINYINT": 1,
    "BIGINT": 8,
    "BOOL": 1,
    "DECIMAL": 8,
}


def col_bytes(dtype, comp_param):
    return int(comp_param / 8) if comp_param != 0 else dtype_sizes[dtype]


def columns(con, table):
    # s = t.schema()
    # cols = con.con.get_table_details(t.name)
    df = omnisci_olio.pymapd.table_details(con.con, table)
    df.rename(
        columns={"name": "column_", "type": "datatype", "precision": "precision_"},
        inplace=True,
    )

    # df['bytesize'] = coldf.apply(lambda row: int(row.comp_param / 8) if row.comp_param != 0 else dtype_sizes[row.type], axis=1)
    df["bytes"] = df.apply(lambda row: col_bytes(row.datatype, row.comp_param), axis=1)
    return df


def schema_columns(con, hostname=None, database=None):
    r = []
    for t in con.list_tables():
        df = columns(con, t)
        df["table_"] = t
        r.append(df)
    df = pd.concat(r)
    df["hostname"] = hostname if hostname else con.host
    df["db"] = database if database else con.database().name
    return df


def load_table_columns(con, table_name="schema_columns"):
    # df = pd.concat([columns(t) for t in con.list_tables()])
    df = all_columns(con)
    con.load_data(table_name, df)
    t = con.table(table_name)
    log.info(table_name, t.count().execute())
    return t


def schema_tables(con, hostname=None, database=None):
    r = []
    for table_name in con.list_tables():
        ddl = pd.read_sql(f"show create table {table_name}", con.con)["Result"].values[
            0
        ]
        with_props = [x for x in ddl.split("\n") if x.startswith("WITH (")]
        tab_props = dict()
        if len(with_props) > 0:
            m = re.match(r"WITH \((.*)\).*", with_props[0])
            if m:
                props = m.groups(1)[0].replace("'", "").split(", ")
                tab_props = {a[0]: a[1] for a in [p.split("=") for p in props]}
        tab_props["table_"] = table_name
        r.append(tab_props)
    df = pd.DataFrame(r)
    for c in ["MAX_ROWS", "FRAGMENT_SIZE"]:
        df[c] = pd.to_numeric(df[c] if c in df else None)
    if "VACUUM" not in df.columns:
        df["VACUUM"] = "delayed"
    df = df[["table_", "FRAGMENT_SIZE", "MAX_ROWS", "VACUUM"]]
    df["hostname"] = hostname if hostname else con.host
    df["db"] = database if database else con.database().name
    return df


def dashboards(con):
    return pd.DataFrame([x.__dict__ for x in con.con.get_dashboards()])


def dashboards_tabs(con):
    r = []
    for board_t in con.con.get_dashboards():
        board = dashboard_json(con, board_t.dashboard_id)
        if "tabs" in board:
            for tab in board["tabs"].values():
                r.append(tab)
        else:
            r.append(board)
    df = pd.DataFrame(
        [
            {
                **board_t.__dict__,
                **{"tabId": tab.get("tabId"), "tabName": tab.get("tabName")},
            }
            for tab in r
        ]
    )
    return df


def dashboard_json(con, id):
    dash = con.con.get_dashboard(id)
    dash_string = base64.b64decode(dash.dashboard_state).decode("utf-8")
    dash_json = json.loads(dash_string)
    return dash_json


def chart_dict(dash, id, chart):
    r = {}
    r["dashboard_id"] = dash.get("id")
    r["dashboard_title"] = dash.get("title")
    r["dashboard_table"] = dash.get("table")
    r["dashboard_version"] = dash.get("version")
    r["dashboard_owner"] = dash.get("owner")

    r["chart_id"] = str(dash.get("id")) + "_" + str(id)
    r["chart_table"] = chart.get("dataSource")
    for key in [
        "title",
        "type",
        "dataSelection",
        "filters",
        "areFiltersInverse",
        "rangeFileter",
        "filterString",
    ]:
        r["chart_" + key] = chart.get(key)

    # r['chart_keys'] = chart.keys()
    # ['dataSource', 'autoSize', 'areFiltersInverse', 'cap', 'renderArea', 'color', 'colorDomain', 'dataSelections', 'selectedLayerId', 'manualPrimaryMeasureDomainMin', 'manualPrimaryMeasureDomainMax', 'manualSecondaryMeasureDomainMin', 'manualSecondaryMeasureDomainMax', 'barColorScheme', 'measureColorScheme', 'vegaSortColumn', 'binSettings', 'densityAccumulatorEnabled', 'dimensions', 'elasticX', 'elasticY', 'filters', 'geoJson', 'loading', 'measures', 'rangeChartEnabled', 'rangeFilter', 'savedColors', 'sortColumn', 'ticks', 'title', 'type', 'showOther', 'showNullDimensions', 'markTypes', 'multiSources', 'legendCollapsed', 'showAbsoluteValues', 'showPercentValues', 'showPercentValuesInPopup', 'showAllOthers', 'hasError', 'isNotDc', 'hoverSelectedColumns', 'width', 'height', 'isLoadingData', 'filterString', 'data', 'yAxisLabel', 'y2AxisLabel', 'basemap', 'numberOfGroups']

    def dim_array(dim):
        agg = dim.get("aggType")
        custom = agg == "Custom"
        return [
            dim.get("table"),
            agg,
            dim.get("value") if not custom else None,
            dim.get("value") if custom else None,
        ]

    r["projections"] = [
        dim_array(dim) for dim in chart.get("dimensions") + chart.get("measures")
    ]

    return r


def dashboard_charts(dash):
    if "tabs" in dash:
        dfs = []
        for tab in dash["tabs"].values():
            c = dashboard_charts(tab)
            if c is not None:
                c["dashboard_tab_id"] = tab.get("tabId")
                c["dashboard_tab_name"] = tab.get("tabName")
                dfs.append(c)
        return pd.concat(dfs)
    elif len(dash["charts"]) > 0:
        # r = {k: chart.get(k) for k in ['title', 'type', 'dataSource']}
        d = [
            chart_dict(dash["dashboard"], id, chart)
            for id, chart in dash["charts"].items()
            if "type" in chart
        ]
        charts = pd.DataFrame(d)
        return charts


def dashboard_chart_projections(charts):
    # print('dashboard_chart_projections', charts.columns)
    projs = charts.explode("projections")
    projs = projs.reset_index()
    projs = projs[projs["projections"].notnull()]
    projs["project_table"] = projs["projections"].apply(lambda x: x[0])
    projs["project_aggregation"] = projs["projections"].apply(lambda x: x[1])
    projs["project_column"] = projs["projections"].apply(lambda x: x[2])
    projs["project_custom"] = projs["projections"].apply(lambda x: x[3])
    del projs["index"]
    del projs["projections"]
    return projs


def dashboard_projections(con, hostname=None, database=None):
    r = []
    for d in con.con.get_dashboards():
        try:
            dash = dashboard_json(con, d.dashboard_id)
            if "tabs" in dash:
                for tab in dash["tabs"].values():
                    c = dashboard_charts(tab)
                    if c is not None:
                        c["dashboard_tab_id"] = tab.get("tabId")
                        c["dashboard_tab_name"] = tab.get("tabName")
                        c = dashboard_chart_projections(c)
                        r.append(c)
            else:
                c = dashboard_charts(dash)
                if c is not None:
                    c = dashboard_chart_projections(c)
                    r.append(c)
        except Exception as e:
            # log.warning('dashboard_id=%s: %s', d.dashboard_id, e)# exc_info=True)
            row = {}
            row["dashboard_id"] = d.dashboard_id
            # row['dashboard_title'] = d.dashboard_title
            #             row['dashboard_table'] = dash.get('table')
            #             row['dashboard_version'] = dash.get('version')
            #             row['dashboard_owner'] = dash.get('owner')
            row["error"] = str(e)
            r.append(pd.DataFrame([row]))

    df = pd.concat(r)
    df["hostname"] = hostname if hostname else con.host
    df["db"] = (
        database
        if database
        else con.con._client.get_session_info(con.con._session).database
    )
    return df
