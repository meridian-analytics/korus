import json
import numpy as np
from .table import encoding_rules, decoding_rules


def to_str(x):
    """ Transform the input to a string, suitably formatted for forming SQLite queries.
    
        Example query: `SELECT * FROM y WHERE z IN {list_to_str(x)}`

        Args:
            x:
                The input 

        Returns:
            : str
                String
    """
    if x is None:
        return "*"
    
    if np.ndim(x) == 0:
        x = [x]

    return "(" + ",".join([f"'{v}'" for v in x]) + ")"


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


def get_column_names(conn, table_name):
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [row[1] for row in rows]


def row_asdict(fields, row):
    return {k: v for k,v in zip(fields, row)}
                                       

def encode_row(table_name, row):
    return {k: encode_field(table_name, k, v) for k,v in row.items() if v is not None}


def decode_row(table_name, row):
    return {k: decode_field(table_name, k, v) for k,v in row.items()}


def fetch_row(conn, table_name, indices=None, fields=None):
    c = conn.cursor()

    if isinstance(fields, str):
        fields = [fields]

    if fields is None:
        fields_str = "*"
    else:
        fields_str = ",".join(fields)

    q = f"SELECT {fields_str} FROM {table_name}"

    if indices is not None:
        q += f" WHERE id IN {to_str(indices)}"

    rows = c.execute(q).fetchall()

    # preserve ordering
    if indices is not None and np.ndim(indices) > 0:
        idx = np.argsort(np.argsort(indices))
        rows = [rows[i] for i in idx]

    return rows

def insert_row(conn, table_name, row):
    """ Insert a row of values into a table in the database.
        
        Args:
            conn: sqlite3.Connection
                Database connection
            table_name: str
                Table name
            row: dict
                row to be inserted
        
        Returns:    
            c: sqlite3.Cursor
                Database cursor
                
        Raises:
            sqlite3.IntegrityError: if the table already contains an entry with these data,
                or a required value is missing, or some other requirement is not fulfilled.
    """
    c = conn.cursor()

    # check if table has its own 'id' column
    has_id = False
    columns = conn.execute(f"PRAGMA table_info({table_name})")
    for col in columns:
        if col[1] == "id":
            has_id = True
            break

    # SQL query
    col_names = ",".join(row.keys())
    val_str = ",".join(["?" for _ in row.keys()])
    if has_id:
        col_names = "id," + col_names
        val_str = "NULL," + val_str

    c.execute(
        f"INSERT INTO {table_name} ({col_names}) VALUES ({val_str})", list(row.values())
    ) 

    return c

def add_column(conn, table_name, col_name, col_type, default_value):
    q = f"ALTER TABLE {table_name} ADD COLUMN {col_name} TEXT"
    if default_value is not None:
        q += f" DEFAULT '{default_value}'"

    conn.execute(q)