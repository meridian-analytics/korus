import json
from datetime import datetime, timezone
import korus.database.backend.sqlite.query as qy


DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"


def encode_condition(table_name, condition, encoder):
    encoded_condition = {}
    for name, values in condition.items():
        is_tuple = isinstance(values, tuple)

        if not isinstance(values, (list, tuple)):
            values = [values]

        values = [encoder(v, table_name, name.replace("~", "")) for v in values]

        if is_tuple:
            values = tuple(values)

        encoded_condition[name] = values

    return encoded_condition


def index_to_key(v: int | list[int]):
    if v is None:
        return None

    elif isinstance(v, int):
        return v + 1

    else:
        return [x + 1 for x in v]


def key_to_index(v: int | list[int]):
    if v is None:
        return None

    if isinstance(v, int):
        return v - 1

    else:
        return [x - 1 for x in v]


def encode_key(v: int | list[int]):
    v = index_to_key(v)
    if isinstance(v, (list, tuple, dict)):
        v = json.dumps(v)

    return v


def decode_key(v: int | list[int]):
    if isinstance(v, str):
        v = json.loads(v)

    return key_to_index(v)


def decode_json(v: str) -> list | tuple | dict:
    if v is None:
        return None

    else:
        return json.loads(v)


def decode_datetime(v: str) -> datetime:
    """Decoder for datetime values.

    Args:
        v: str
            The encoded datetime value. Must have the format `%Y-%m-%d %H:%M:%S.%f`

    Returns:
        : datetime
            The decoded datetime object in UTC timezone.
            None, if the input string is None.
    """
    if v is None:
        return None

    else:
        return datetime.strptime(v, DATETIME_FORMAT).replace(tzinfo=timezone.utc)


def encode_field(value: "typing.Any", fcn: callable = None):
    """Encode any input value.

    If no encoding function is specified, the following default encoding
    rules are enforced,

        * tuples, lists, and dicts are encoded using `json.dumps`
        * datetime objects are encoded as strings using the format `%Y-%m-%d %H:%M:%S.%f`
        * all other input types are returned unchanged

    Args:
        value: typing.Any
            The value to be encoded
        fcn: callable (optional)
            Encoding function.

    Returns:
        : typing.Any
            The encoded value
    """

    if fcn is not None:
        return fcn(value)

    elif isinstance(value, (tuple, list, dict)):
        return json.dumps(value)

    elif isinstance(value, datetime):
        return value.strftime(DATETIME_FORMAT)

    else:
        return value


def decode_field(value, fcn=None):
    if fcn is not None:
        return fcn(value)

    else:
        return value


def encode_row(row, fcns=None):
    if fcns is None:
        fcns = {}

    return {
        k: encode_field(v, fcns.get(k, None)) for k, v in row.items() if v is not None
    }


def decode_row(row, fcns=None):
    if fcns is None:
        fcns = {}

    return {k: decode_field(v, fcns.get(k, None)) for k, v in row.items()}


class RuleSet:
    def __init__(self):
        self.rules = dict()

    def add_rule(self, table_name, col_name, fcn):
        key = self._key(table_name, col_name)
        self.rules[key] = fcn

    def get_rule(self, table_name, col_name):
        key = self._key(table_name, col_name)
        return self.rules.get(key, None)

    def get_rules(self, x, table_name):
        return {col_name: self.get_rule(table_name, col_name) for col_name in x}

    def _key(self, table_name, col_name):
        return (table_name, col_name)


class Encoder(RuleSet):
    def __call__(self, x, table_name=None, col_name=None):
        if col_name is None:
            fcns = self.get_rules(x, table_name)
            return encode_row(x, fcns)

        else:
            fcn = self.get_rule(table_name, col_name)
            return encode_field(x, fcn)


class Decoder(RuleSet):
    def __call__(self, x, table_name=None, col_name=None):
        if col_name is None:
            fcns = self.get_rules(x, table_name)
            return decode_row(x, fcns)

        else:
            fcn = self.get_rule(table_name, col_name)
            return decode_field(x, fcn)


class Codec:
    def __init__(self):
        self.decoder = Decoder()
        self.encoder = Encoder()

    def encode(self, *args, **kwargs):
        return self.encoder(*args, **kwargs)

    def decode(self, *args, **kwargs):
        return self.decoder(*args, **kwargs)


def create_codec(conn):
    codec = Codec()

    # decode JSON columns
    for tbl_name in qy.get_table_names(conn):
        col_types = qy.get_column_types(conn, tbl_name)
        for col_name, col_type in col_types.items():
            if col_type == "JSON":
                codec.decoder.add_rule(tbl_name, col_name, decode_json)

    # decode timestamps
    codec.decoder.add_rule("deployment", "start_utc", decode_datetime)
    codec.decoder.add_rule("deployment", "end_utc", decode_datetime)
    codec.decoder.add_rule("file", "start_utc", decode_datetime)
    codec.decoder.add_rule("file", "end_utc", decode_datetime)
    codec.decoder.add_rule("taxonomy", "timestamp", decode_datetime)

    # encode & decode file_id_list field in annotation table
    codec.encoder.add_rule("annotation", "file_id_list", encode_key)
    codec.decoder.add_rule("annotation", "file_id_list", decode_key)

    # encode & decode all fields named *_id
    table_names = qy.get_table_names(conn)
    for table_name in table_names:
        # primary keys
        codec.encoder.add_rule(table_name, "id", encode_key)
        codec.decoder.add_rule(table_name, "id", decode_key)

        for key_name in table_names:
            # foreign keys
            key_name += "_id"
            codec.encoder.add_rule(table_name, key_name, encode_key)
            codec.decoder.add_rule(table_name, key_name, decode_key)

    return codec
