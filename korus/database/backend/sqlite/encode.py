import json
from datetime import datetime, timezone
    

encoding_rules = dict()
decoding_rules = dict()


def add_encoding_rule(table_name, col_name, fcn):
    encoding_rules[(table_name, col_name)] = fcn

def add_decoding_rule(table_name, col_name, fcn):
    decoding_rules[(table_name, col_name)] = fcn


datetime_fmt = "%Y-%m-%d %H:%M:%S.%f"

def encode_datetime(v):
    return v.strftime(datetime_fmt)

def decode_datetime(v):
    return datetime.strptime(v, datetime_fmt).replace(tzinfo=timezone.utc)
    

def encode_field(table_name, col_name, value):
    if value is None:
        return None
    
    key = (table_name, col_name)
    if key in encoding_rules:
        return encoding_rules[key](value)

    if isinstance(value, (int,float)):
        return f"{value}"

    elif isinstance(value, (tuple,list,dict)):
        return json.dumps(value)
    
    else:
        return value


def decode_field(table_name, col_name, value):
    if value is None:
        return None

    key = (table_name, col_name)
    if key in decoding_rules:
        return decoding_rules[key](value)

    return value


def encode_row(table_name, row):
    return {k: encode_field(table_name, k, v) for k,v in row.items() if v is not None}


def decode_row(table_name, row):
    return {k: decode_field(table_name, k, v) for k,v in row.items()}


