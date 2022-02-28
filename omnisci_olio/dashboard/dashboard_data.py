import os
import re
import pandas as pd
import logging
import base64
import json

logging.basicConfig(level=logging.INFO)
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
    return int(comp_param / 8) if comp_param != 0 else dtype_sizes.get(dtype, -1)


def columns(con, table):
    # s = t.schema()
    df = pd.DataFrame(con.con.get_table_details(table))
    df.rename(
        columns={"name": "column_name", "type": "datatype", "precision": "precision_"},
        inplace=True,
    )

    # df['bytesize'] = coldf.apply(lambda row: int(row.comp_param / 8) if row.comp_param != 0 else dtype_sizes[row.type], axis=1)
    df["bytes"] = df.apply(lambda row: col_bytes(row.datatype, row.comp_param), axis=1)
    return df


def schema_columns(con, hostname=None, schema_name=None):
    r = []
    for t in con.list_tables():
        try:
            df = columns(con, t)
        except Exception as e:
            # log.warning('dashboard_id=%s: %s', d.dashboard_id, e)# exc_info=True)
            df = pd.DataFrame(
                [{"column_error": str(e), "nullable": False, "is_array": False}]
            )
        df["table_name"] = t
        r.append(df)
    if len(r) == 0:
        return None
    df = pd.concat(r)
    df["hostname"] = hostname if hostname else con.host
    df["schema_name"] = (
        schema_name
        if schema_name
        else con.con._client.get_session_info(con.con._session).database
    )
    return df


def schema_tables(con, hostname=None, schema_name=None):
    df = pd.read_sql("show table details", con.con)
    df["hostname"] = hostname if hostname else con.host
    df["schema_name"] = (
        schema_name
        if schema_name
        else con.con._client.get_session_info(con.con._session).database
    )
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
        if chart.get(key) and len(chart.get(key)) > 32767:
            r["chart_" + key] = chart.get(key)[:32767]

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
    projs["project_column"] = projs["projections"].apply(
        lambda x: x[2][:32767] if x[2] else None
    )
    projs["project_custom"] = projs["projections"].apply(
        lambda x: x[3][:32767] if x[3] else None
    )
    del projs["index"]
    del projs["projections"]
    return projs


def dashboard_projections(con, hostname=None, schema_name=None):
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
                        c["dashboard_id"] = d.dashboard_id

                        c["error"] = None
                        r.append(c)
            else:
                c = dashboard_charts(dash)
                if c is not None:
                    c = dashboard_chart_projections(c)
                    c["dashboard_id"] = d.dashboard_id

                    c["error"] = None
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
    if len(r) > 0:
        df = pd.concat(r)
        # df['dashboard_id'] = pd.to_numeric(df['dashboard_id'])
        df["hostname"] = hostname if hostname else con.host
        df["schema_name"] = (
            schema_name
            if schema_name
            else con.con._client.get_session_info(con.con._session).database
        )
        return df
    else:
        return None


def load_omnisci_schema(
    src_con, con, hostname=None, schema_name=None, drop=False, load_dashboards=True
):
    column_tn = "schema_column"
    table_tn = "schema_table"
    tab_col_join_cols = ["hostname", "schema_name", "table_name"]
    tc_vn = "schema_tables_column"
    dp_tn = "schema_dashboard_projection"
    dp_vn = "schema_dashboard_projection_v"

    if drop:
        con.drop_table(column_tn, force=True)
        con.drop_table(table_tn, force=True)
        con.drop_view(tc_vn, force=True)
        if load_dashboards:
            con.drop_table(dp_tn, force=True)
            con.drop_view(dp_vn, force=True)

    # CREATE TABLES
    create_tables = {
        column_tn: f"""CREATE TABLE {column_tn} (
            column_name TEXT ENCODING DICT(32),
            datatype TEXT ENCODING DICT(32),
            nullable BOOLEAN,
            precision_ BIGINT,
            scale BIGINT,
            comp_param BIGINT,
            encoding TEXT ENCODING DICT(32),
            is_array BOOLEAN,
            bytes BIGINT,
            table_name TEXT ENCODING DICT(32),
            column_error TEXT ENCODING DICT(32),
            hostname TEXT ENCODING DICT(32),
            schema_name TEXT ENCODING DICT(16))
        """,
        table_tn: f"""CREATE TABLE {table_tn} (
            table_id BIGINT,
            table_name TEXT ENCODING DICT(32),
            column_count BIGINT,
            is_sharded_table BIGINT,
            shard_count BIGINT,
            max_rows BIGINT,
            fragment_size BIGINT,
            max_rollback_epochs BIGINT,
            min_epoch BIGINT,
            max_epoch BIGINT,
            min_epoch_floor BIGINT,
            max_epoch_floor BIGINT,
            metadata_file_count BIGINT,
            total_metadata_file_size BIGINT,
            total_metadata_page_count BIGINT,
            total_free_metadata_page_count DOUBLE,
            data_file_count BIGINT,
            total_data_file_size BIGINT,
            total_data_page_count BIGINT,
            total_free_data_page_count DOUBLE,
            hostname TEXT ENCODING DICT(32),
            schema_name TEXT ENCODING DICT(16))
        """,
        dp_tn: f"""CREATE TABLE {dp_tn} (
            dashboard_id INT,
            dashboard_title TEXT ENCODING DICT(32),
            dashboard_table TEXT ENCODING DICT(32),
            dashboard_version TEXT ENCODING DICT(8),
            dashboard_owner TEXT ENCODING DICT(16),
            chart_id TEXT ENCODING DICT(32),
            chart_table TEXT ENCODING DICT(32),
            chart_title TEXT ENCODING DICT(32),
            chart_type TEXT ENCODING DICT(16),
            chart_dataSelection TEXT ENCODING DICT(32),
            chart_filters TEXT ENCODING DICT(32),
            chart_areFiltersInverse TEXT ENCODING DICT(32),
            chart_rangeFileter TEXT ENCODING DICT(32),
            chart_filterString TEXT ENCODING DICT(32),
            project_table TEXT ENCODING DICT(32),
            project_aggregation TEXT ENCODING DICT(32),
            project_column TEXT ENCODING DICT(32),
            project_custom TEXT ENCODING DICT(32),
            dashboard_tab_id TEXT ENCODING DICT(32),
            dashboard_tab_name TEXT ENCODING DICT(32),
            error TEXT ENCODING DICT(32),
            hostname TEXT ENCODING DICT(32),
            schema_name TEXT ENCODING DICT(16))
        """,
    }
    for tn, ct in create_tables.items():
        if not con.exists_table(tn):
            con.con.execute(ct)

    df = schema_columns(src_con, hostname=hostname, schema_name=schema_name)
    tab = None
    if df is not None:
        col = load_table(con, column_tn, df)

        df = schema_tables(src_con, hostname=hostname, schema_name=schema_name)
        tab = load_table(con, table_tn, df)

        if not con.exists_table(tc_vn):
            join = tab.join(
                col, predicates=[(tab[c] == col[c]) for c in tab_col_join_cols]
            )
            sel = join.select(
                [tab[c] for c in tab_col_join_cols]
                + [tab[c] for c in tab.columns if c not in tab_col_join_cols]
                + [col[c] for c in col.columns if c not in tab_col_join_cols]
            )
            con.create_view(tc_vn, sel)

    if load_dashboards and tab is not None:
        dp = con.table(dp_tn)
        df = dashboard_projections(src_con, hostname=hostname, schema_name=schema_name)
        if df is not None:
            dp = load_table(con, dp_tn, df)

            if not con.exists_table(dp_vn):
                join_pc = dp.inner_join(
                    col,
                    (col.hostname == dp.hostname)
                    & (col.schema_name == dp.schema_name)
                    & (col.table_name == dp.project_table),
                )
                join_pct = join_pc.inner_join(
                    tab, [(tab[c] == col[c]) for c in tab_col_join_cols]
                )
                sel = join_pct.select(
                    [dp]
                    + [tab[c] for c in tab.columns if c not in tab_col_join_cols]
                    + [col[c] for c in col.columns if c not in tab_col_join_cols]
                )
                con.create_view(dp_vn, sel)


def load_table(con, tn, df):
    t = con.table(tn)
    for c in t.columns:
        if c not in df:
            df[c] = None
    df = df[t.columns]
    df = t.schema().apply_to(df)
    con.load_data(tn, df)
    log.info("load_table %s %s", tn, t.count().execute())
    return t


def databases(con):
    return [d.db_name for d in con.con._client.get_databases(con.con._session)]


def load_omnisci_schema_all(
    src_con, tgt_con, hostname=None, load_dashboards=True, drop=False
):
    for schema_name in databases(src_con):
        log.info("load_omnisci_schema_all %s", schema_name)
        src_con.con._client.switch_database(src_con.con._session, schema_name)
        load_omnisci_schema(
            src_con,
            tgt_con,
            hostname=hostname,
            schema_name=schema_name,
            drop=drop,
            load_dashboards=load_dashboards,
        )
        drop = False  # only the first time
