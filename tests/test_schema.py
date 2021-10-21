import omnisci_olio.schema as sc
from omnisci_olio.workflow import connect


def test_schema():
    ddl = """\
CREATE TABLE test_schema (
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

    ts = sc.Table(
        "test_schema",
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
    assert ddl == ts.compile()

    with connect() as con:
        con.create_table(ts.name, ts, drop=True)

        assert ddl == con.con.con.execute(f"show create table {ts.name}").fetchone()[0]
