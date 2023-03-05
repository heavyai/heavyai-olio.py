"""
A simple object structure to generate HEAVYAI DB DDL definitions.
"""

import re
import copy
import numpy as np
import pandas as pd

# warning: this is not a public API
import heavyai._pandas_loaders as heavyai_loaders


class ModelObject:

    def __init__(
        self,
        name,
        tags=None,
    ):
        self.name = name
        self.tags = tags


class ModelOperation:

    def __init__(self, model, sql, category) -> None:
        self.model = model
        self.sql = sql
        self.category = category
    
    def compile(self):
        return self.sql
    
    def model_table(self):
        if self.model.hasattr('table'):
            return self.model.table
        else:
            return self.model


datatypes = []

class Datatype:
    def __init__(
        self,
        typename,
        size=32,
        nullable=True,
        precision=None,
        scale=None,
        encoding=None,
        array=False,
        array_length=None,
        alt_name=None,
    ):
        self.typename = typename
        self.size = size
        self.nullable = nullable
        self.precision = precision
        self.scale = scale
        self.encoding = encoding
        self.array = array
        self.array_length = array_length
        self.alt_name = alt_name

        datatypes.append(self)

    # def __str__(self):
    #     # TODO this is not correct for all datatypes, must be defined in subclass
    #     enc = "ENCODING {self.encoding}({self.size}, {self.precision}, {self.scale})"
    #     arr = ("[{self.array_length}]" if self.array else "")
    #     return f"{self.typename} {enc} {arr}"

    def __str__(self):
        if self.array:
            return f"{self.typename}[{self.array_length or ''}]"
        else:
            return self.typename

    def copy_with(self,
        array=False,
    ):
        c = copy.deepcopy(self)
        if array is not None:
            c.array = array
        return c


class Text(Datatype):
    def __init__(self, size=32, encoding="DICT", array=False, alt_name=None):
        super().__init__("TEXT", size=size, encoding=encoding, array=array, alt_name=alt_name)

    def __str__(self):
        if self.array:
            if self.size != 32 or self.encoding != "DICT":
                raise Exception("HEAVYAI supports only DICT(32) TEXT arrays")
            return "TEXT[] ENCODING DICT(32)"
        elif self.encoding is None:
            return "TEXT ENCODING NONE"
        else:
            return f"TEXT ENCODING {self.encoding}({self.size})"
            # return "TEXT ENCODING" + self.encoding + "(" + self.size + ")"


# text
text_enc_none = Text(encoding=None)
text8 = Text(8)
text16 = Text(16)
text32 = Text(32, alt_name="STR")


class Boolean(Datatype):

    def __init__(self, array=False):
        super().__init__("BOOLEAN", array=array, alt_name="BOOL")

# boolean
boolean = Boolean()

# float
class Float(Datatype):
    def __init__(self, size=32, array=False, array_length=None):
        if size == 32:
            typename = "FLOAT"
        elif size == 64:
            typename = "DOUBLE"
        else:
            raise Exception("Float size only supports 32 or 64")
        super().__init__(
            typename=typename, size=size, array=array, array_length=array_length
        )

float32 = Float()
float64 = Float(64)

_int_sizes = {
    8: "TINYINT",
    16: "SMALLINT",
    32: "INTEGER",
    64: "BIGINT",
}

# int
class Integer(Datatype):
    def __init__(self, size=32, array=False, array_length=None, alt_name=None):
        super().__init__(
            _int_sizes[size],
            encoding="FIXED",
            size=size,
            array=array,
            array_length=array_length,
            alt_name=alt_name
        )

int8 = Integer(8)
int16 = Integer(16)
int32 = Integer(alt_name="INT")
int64 = Integer(64)

# time
class Timestamp(Datatype):
    def __init__(self, precision=0, size=64, array=False, array_length=None):
        super().__init__(
            "TIMESTAMP",
            encoding="FIXED",
            size=size,
            precision=precision,
            array=array,
            array_length=array_length,
        )

    def __str__(self):
        arr = "[{self.array_length}]" if self.array else ""
        if self.size == 32:
            return f"{self.typename} ENCODING {self.encoding}({self.size}){arr}"
        else:
            return f"{self.typename}({self.precision}){arr}"


timestamp0ef32 = Timestamp(0, 32)
# timestamp9 = timestamp(9)

# date
class Date(Datatype):
    def __init__(self, encoding="DAYS", size=32, array=False, array_length=None):
        super().__init__(
            "DATE",
            encoding=encoding,
            size=size,
            array=array,
            array_length=array_length,
        )

    def __str__(self):
        arr = "[{self.array_length}]" if self.array else ""
        if self.size == 32:
            return f"{self.typename} ENCODING {self.encoding}({self.size}){arr}"
        else:
            return f"{self.typename}({self.precision}){arr}"


days32 = Date("DAYS", 32)

# geo
class Geometry(Datatype):
    def __init__(self, shape, srid, compressed=None, array=False, array_length=None):
        super().__init__(
            "GEOMETRY",
            encoding="COMPRESSED" if compressed else None,
            size=None,
            array=array,
            array_length=array_length,
            alt_name=shape,
        )
        self.compressed = compressed
        self.shape = shape
        self.srid = srid

    def __str__(self):
        if self.compressed:
            return f"GEOMETRY({self.shape}, {self.srid}) ENCODING {self.encoding}({self.compressed})"
        else:
            return f"GEOMETRY({self.shape}, {self.srid})"


point4326ec32 = Geometry("POINT", 4326, 32)
polygon4326ec32 = Geometry("POLYGON", 4326, 32)
linestring4326ec32 = Geometry("LINESTRING", 4326, 32)
multipolygon4326ec32 = Geometry("MULTIPOLYGON", 4326, 32)
multilinestring4326ec32 = Geometry("MULTILINESTRING", 4326, 32)

# TODO more types

class Column (ModelObject):
    def __init__(
        self,
        name,
        datatype,
        shared_dict=None,
        shard_key=False,
        comment=None,
        source_col=None,
        tags=None,
        rename_from=None,
        is_drop=False,
    ):
        super().__init__(name=name, tags=tags)
        self.datatype = datatype
        self.comment = comment
        self.source_col = source_col
        if shared_dict:
            if isinstance(shared_dict, Column):
                self.shared_dict = shared_dict
            elif isinstance(shared_dict, Table):
                # for convenience, lookup column with same name
                self.shared_dict = shared_dict[self.name]
            else:
                raise Exception(f"Invalid Column for shared_dict {shared_dict}")
            if self.datatype != self.shared_dict.datatype:
                raise Exception(
                    f"Shared dict datatype must be the same: {self.datatype} != {self.shared_dict.datatype}"
                )
        else:
            self.shared_dict = None
        self.shard_key = shard_key
        self.table = None
        self.rename_from = rename_from
        self.is_drop = is_drop

    def to_heavyai_dtype(series):
        try:
            return heavyai_loaders.get_mapd_dtype(series)
        except TypeError as e:
            if pd.api.types.is_object_dtype(series):
                try:
                    val = series.dropna().iloc[0]
                except IndexError:
                    raise IndexError("Not any valid values to infer the type")

                # TODO add this check for np.ndarray to heavyai._pandas_loaders get_mapd_type_from_object
                if isinstance(val, np.ndarray):
                    return 'ARRAY/{}'.format(heavyai_loaders.get_mapd_dtype(pd.Series(list(val))))
                if isinstance(val, set):
                    return 'ARRAY/{}'.format(heavyai_loaders.get_mapd_dtype(pd.Series(list(val))))
            raise Exception(str(dict(val=val, type=type(val)))) from e

    def from_dataframe(name, series):
        try:
            hdt = Column.to_heavyai_dtype(series)
            is_array = hdt.startswith('ARRAY')
            hdt = hdt.replace('ARRAY/', '')

            for d in datatypes:
                if hdt == d.typename or hdt == d.alt_name:
                    return Column(name, d.copy_with(array=is_array))
        except Exception as e:
            raise Exception(name) from e
        raise Exception(f"unknown datatype {hdt} for {name}")

    def compile(self):
        return f"{self.name} {self.datatype}"

    def define(self):
        return ModelOperation(self, self.compile(), "DEFINE")

    def compile_shared_dict(self):
        if self.shared_dict:
            return f"SHARED DICTIONARY ({self.name}) REFERENCES {self.shared_dict.table.name}({self.shared_dict.name})"
        else:
            return None

    def shared_dict(self):
        return ModelOperation(self, self.compile_shared_dict(), "DEFINE")

    def compile_shard_key(self):
        if self.shared_dict:
            return f"SHARD KEY ({self.name})"
        else:
            return None
    
    def shared_dict(self):
        return ModelOperation(self, self.compile_shard_key(), "DEFINE")

    def compile_add(self):
        return f"""ALTER TABLE {self.table.name} ADD COLUMN {self.compile()}"""

    def add(self):
        return ModelOperation(self, self.compile_add(), "DEFINE")
    
    def drop(self):
        return ModelOperation(self, f"ALTER TABLE {self.table.name} DROP COLUMN {self.name}", "DROP")

    def compile_rename(self):
        return f"""ALTER TABLE {self.table.name} RENAME COLUMN {self.rename_from} TO {self.name}"""

    def rename(self):
        return ModelOperation(self, self.compile_rename(), "RENAME")

    def __str__(self):
        return self.compile()


class Table (ModelObject):
    def __init__(
        self, name, columns=None, description=None, props=None, temp=False, tags=None, **kwargs
    ):
        """
        columns - dict of either (name:str, datatype:Datatype) or list of Column
        kwargs - column (name = datatype)
        """
        super().__init__(name=name, tags=tags)
        self.props = props
        self.temp = temp

        if columns is None:
            columns = []
        for k, v in kwargs.items():
            if isinstance(v, tuple):
                columns.append(Column(k, *v))
            else:
                columns.append(Column(k, v))
        self.columns = columns

        for c in self.columns:
            c.table = self
    
    def from_dataframe(name, df):
        return Table(name, [Column.from_dataframe(col, df[col]) for col in df.columns])

    def copy_named(self, name):
        c = copy.deepcopy(self)
        c.name = name
        return c

    def __getitem__(self, col_name):
        for c in self.columns:
            if c.name == col_name:
                return c
        raise KeyError(f"No column named '{col_name}'")

    def __get__(self, col_name):
        for c in self.columns:
            if c.name == col_name:
                return c
        raise AttributeError(f"No column named '{col_name}'")

    def _compile_with_props(self, kwargs):
        if not kwargs:
            return ""

        def val(v):
            if isinstance(v, str):
                return f"'{v}'"
            elif isinstance(v, bool):
                return "'true'" if v else "'false'"
            else:
                return str(v)

        props = [f"{k.upper()}={val(v)}" for k, v in kwargs.items() if v is not None]
        if len(props) > 0:
            props = ", ".join(props)
            return f"WITH ({props})"
        else:
            return ""

    def compile(self, name=None):
        """
        name - if not None, use name instead of self.name
        """
        name = name or self.name
        cols = [f"  {c.compile()}" for c in self.columns]
        cols += [f"  {c.compile_shard_key()}" for c in self.columns if c.shard_key]
        cols += [f"  {c.compile_shared_dict()}" for c in self.columns if c.shared_dict]
        cols = ",\n".join(cols)
        table = "TABLE" if not self.temp else "TEMPORARY TABLE"
        ddl = f"""CREATE {table} {name} (
{cols})
{self._compile_with_props(self.props)};"""
        return ddl

    def define(self) -> ModelOperation:
        return ModelOperation(self, self.compile(), "DEFINE")

    def show_def(self) -> ModelOperation:
        return ModelOperation(self, f"SHOW CREATE TABLE {self.name}", "SHOW")

    def __str__(self):
        return self.compile()


parse_datatypes = {
    "TEXT ENCODING DICT(8)": "Text(8)",
    "TEXT ENCODING DICT(16)": "Text(16)",
    "TEXT ENCODING DICT(32)": "Text(32)",
    "TINYINT": "Integer(8)",
    "SMALLINT": "Integer(16)",
    "INTEGER": "Integer(32)",
    "BIGINT": "Integer(64)",
    "FLOAT": "Float(32)",
    "DOUBLE": "Float(64)",
    # TODO more types
}


def parse_ddl_to_python(ddl_text, namespace="sc"):
    """
    Parse HEAVYAI DDL text, print python code for a Table and Columns.
    """
    table_name = None
    cols = []
    ddl_text = ddl_text.strip()
    if ddl_text.endswith(";"):
        ddl_text = ddl_text[:-1]
    result = []
    for line in ddl_text.split("\n"):
        try:
            if line.startswith("CREATE TABLE "):
                m = re.match("CREATE TABLE (.*) \\(", line)
                table_name = m[1]
                result.append(f"""{namespace}.Table("{table_name}", [""")
            elif line.startswith("SHARED DICTIONARY "):
                m = re.match("SHARED DICTIONARY \\((.*)\\) REFERENCES (.*)\\((.*)\\)", line)
                # TODO SHARED DICTIONARY
                result.append(f"""TODO SHARED DICTIONARY {m[1]} {m[2]} {m[3]}""")
            elif line.startswith("SHARD KEY "):
                m = re.match("SHARD KEY \\((.*)\\)", line)
                # TODO SHARD KEY
                result.append(f"""TODO SHARD KEY {m[1]}""")
            else:
                m = re.match(" *([a-zA-Z0-9_]*) (.*)[,\\)]$", line)
                dt = parse_datatypes.get(m[2], m[2])
                result.append(f"""    {namespace}.Column("{m[1]}", {namespace}.{dt}),""")
        except Exception as e:
            raise Exception(f'line = "{line}"') from e
    result.append("])")
    return "\n".join(result)
