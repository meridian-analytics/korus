import json
from datetime import datetime, timezone
    

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"


def decode_datetime(v):
    if v is None:
        return None
    
    else:
        return datetime.strptime(v, DATETIME_FORMAT).replace(tzinfo=timezone.utc)
    

def encode_field(value, fcn = None):
    if fcn is not None:
        return fcn(value)
    
    elif isinstance(value, (tuple,list,dict)):
        return json.dumps(value)
    
    elif isinstance(value, datetime):
        return value.strftime(DATETIME_FORMAT)
    
    else:
        return value


def decode_field(value, fcn = None):
    if fcn is not None:
        return fcn(value)
    
    else:
        return value


def encode_row(row, fcns):
    return {k: encode_field(v, fcns.get(k, None)) for k,v in row.items() if v is not None}


def decode_row(row, fcns):
    return {k: decode_field(v, fcns.get(k, None)) for k,v in row.items()}


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