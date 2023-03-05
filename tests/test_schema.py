import heavyai_olio.schema as sc
from heavyai_olio.workflow import connect


test1_ddl = """\
CREATE TABLE test_schema_datatypes (
  text_ TEXT ENCODING DICT(32),
  text_none_ TEXT ENCODING NONE,
  text_8_ TEXT ENCODING DICT(8),
  text_array_ TEXT[] ENCODING DICT(32),
  int_ INTEGER,
  int64_ BIGINT,
  float_ FLOAT,
  double_ DOUBLE,
  timestamp_ TIMESTAMP(0),
  timestamp_9_ TIMESTAMP(9),
  point_4326_32_ GEOMETRY(POINT, 4326) ENCODING COMPRESSED(32))
WITH (FRAGMENT_SIZE=1000000, MAX_ROWS=2000000, MAX_ROLLBACK_EPOCHS=13);"""

test1_tbl = sc.Table(
    "test_schema_datatypes",
    [
        sc.Column("text_", sc.Text()),
        sc.Column("text_none_", sc.Text(encoding=None)),
        sc.Column("text_8_", sc.Text(8)),
        sc.Column("text_array_", sc.Text(array=True)),
        sc.Column("int_", sc.Integer()),
        sc.Column("int64_", sc.Integer(64)),
        sc.Column("float_", sc.Float()),
        sc.Column("double_", sc.Float(64)),
        sc.Column("timestamp_", sc.Timestamp()),
        sc.Column("timestamp_9_", sc.Timestamp(9)),
        sc.Column("point_4326_32_", sc.Geometry("POINT", 4326, 32)),
    ],
    props=dict(fragment_size=1000000, max_rows=2000000, max_rollback_epochs=13),
)

test1_code_text = """\
sc.Table("test_schema_datatypes", [
    sc.Column("text_", sc.Text(32)),
    sc.Column("text_none_", sc.TEXT ENCODING NONE),
    sc.Column("text_8_", sc.Text(8)),
    sc.Column("text_array_", sc.TEXT[] ENCODING DICT(32)),
    sc.Column("int_", sc.Integer(32)),
    sc.Column("int64_", sc.Integer(64)),
    sc.Column("float_", sc.Float(32)),
    sc.Column("double_", sc.Float(64)),
    sc.Column("timestamp_", sc.TIMESTAMP(0)),
    sc.Column("timestamp_9_", sc.TIMESTAMP(9)),
    sc.Column("point_4326_32_", sc.GEOMETRY(POINT, 4326) ENCODING COMPRESSED(32)),
    sc.Column("WITH", sc.(FRAGMENT_SIZE=1000000, MAX_ROWS=2000000, MAX_ROLLBACK_EPOCHS=13),
])"""

def test_schema_datatypes():
    assert test1_ddl == test1_tbl.define().compile()

    with connect() as con:
        con.create_table(test1_tbl, drop=True)

        assert test1_ddl == con.query1(test1_tbl.show_def())

def test_schema_evolution():
    with connect() as con:
        tbl_a = sc.Table(
            "test_schema_evolution",
            [
                sc.Column("col_a", sc.Integer()),
            ],
            props=dict(fragment_size=100, max_rows=2000000),
        )
        tbl_b = sc.Table(
            "test_schema_evolution",
            [
                sc.Column("col_b", sc.Integer(), rename_from="col_a"),
                sc.Column("col_c", sc.Integer()),
            ],
            props=dict(fragment_size=100, max_rows=2000000),
        )

        con.create_table(tbl_a, drop=True)
        t = con.table(tbl_a)
        assert ["col_a"] == t.columns

        con.create_table(tbl_b, drop=False)
        t = con.table(tbl_b)
        assert ["col_b", "col_c"] == t.columns

def test_parse_ddl_to_python():
    code_text = sc.parse_ddl_to_python(test1_ddl)
    assert test1_code_text == code_text
