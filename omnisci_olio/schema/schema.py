"""
A simple object structure to generate OmniSciDB DDL definitions.
"""


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
    ):
        self.typename = typename
        self.size = size
        self.nullable = nullable
        self.precision = precision
        self.scale = scale
        self.encoding = encoding
        self.array = array
        self.array_length = array_length

    # def __str__(self):
    #     # TODO this is not correct for all datatypes, must be defined in subclass
    #     enc = "ENCODING {self.encoding}({self.size}, {self.precision}, {self.scale})"
    #     arr = ("[{self.array_length}]" if self.array else "")
    #     return f"{self.typename} {enc} {arr}"


class Text(Datatype):
    def __init__(self, size=32, encoding="DICT", array=False):
        super().__init__("TEXT", size=size, encoding=encoding, array=array)

    def __str__(self):
        if self.array:
            if self.size != 32 or self.encoding != "DICT":
                raise Exception("OmniSci supports only DICT(32) TEXT arrays")
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
text32 = Text(32)


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

    def __str__(self):
        if self.array:
            return f"{self.typename}[{self.array_length or ''}]"
        else:
            return self.typename


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
    def __init__(self, size=32, array=False, array_length=None):
        super().__init__(
            _int_sizes[size],
            encoding="FIXED",
            size=size,
            array=array,
            array_length=array_length,
        )

    def __str__(self):
        if self.array:
            return f"{self.typename}[{array_length or ''}]"
        else:
            return self.typename


int8 = Integer(8)
int16 = Integer(16)
int32 = Integer()
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

# geo
class Geometry(Datatype):
    def __init__(self, shape, srid, compressed=None, array=False, array_length=None):
        super().__init__(
            "GEOMETRY",
            encoding="COMPRESSED" if compressed else None,
            size=None,
            array=array,
            array_length=array_length,
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

# TODO more types


class Column:
    def __init__(
        self,
        name,
        datatype,
        shared_dict=None,
        shard_key=False,
        comment=None,
        source_col=None,
    ):
        self.name = name
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

    def compile(self):
        return f"{self.name} {self.datatype}"

    def compile_shared_dict(self):
        if self.shared_dict:
            return f"SHARED DICTIONARY ({self.name}) REFERENCES {self.shared_dict.table.name}({self.shared_dict.name})"
        else:
            return None

    def compile_shard_key(self):
        if self.shared_dict:
            return f"SHARD KEY ({self.name})"
        else:
            return None

    def __str__(self):
        return self.compile()


class Table:
    def __init__(
        self, name, columns=None, description=None, props=None, temp=False, **kwargs
    ):
        """
        columns - dict of either (name:str, datatype:Datatype) or list of Column
        kwargs - column (name = datatype)
        """
        self.name = name
        self.props = props
        self.temp = temp

        if columns is None:
            columns = []
        for k, v in kwargs:
            if isinstance(v, tuple):
                columns.append(Column(k, *v))
            else:
                columns.append(Column(k, v))
        self.columns = columns

        for c in self.columns:
            c.table = self

    def __getitem__(self, key):
        for c in self.columns:
            if c.name == key:
                return c
        raise KeyError(key)

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
    Parse OmniSci DDL text, print python code for a Table and Columns.
    """
    table_name = None
    cols = []
    ddl_text = ddl_text.strip()
    if ddl_text.endswith(";"):
        ddl_text = ddl_text[:-1]
    for line in ddl_text.split("\n"):
        try:
            if line.startswith("CREATE TABLE "):
                m = re.match("CREATE TABLE (.*) \(", line)
                table_name = m[1]
                print(f"""{namespace}.Table("{table_name}", [""")
            elif line.startswith("SHARED DICTIONARY "):
                m = re.match("SHARED DICTIONARY \((.*)\) REFERENCES (.*)\((.*)\)", line)
                # TODO SHARED DICTIONARY
                print(f"""TODO SHARED DICTIONARY {m[1]} {m[2]} {m[3]}""")
            elif line.startswith("SHARD KEY "):
                m = re.match("SHARD KEY \((.*)\)", line)
                # TODO SHARD KEY
                print(f"""TODO SHARD KEY {m[1]}""")
            else:
                m = re.match(" *([a-zA-Z0-9_]*) (.*)[,\)]$", line)
                dt = parse_datatypes.get(m[2], m[2])
                print(f"""    {namespace}.Column("{m[1]}", {namespace}.{dt}),""")
        except Exception as e:
            raise Exception(f'line = "{line}"') from e
    print("])")
