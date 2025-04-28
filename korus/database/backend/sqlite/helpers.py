import json
import datetime
import numpy as np

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

def encode_field(v):
    if v is None:
        return None

    elif isinstance(v, bool):
        return 1 if v else 0
    
    elif isinstance(v, str):
        return v
    
    elif isinstance(v, (int,float)):
        return f"{v}"
    
    elif isinstance(v, datetime.datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S.%f")

    elif isinstance(v, (tuple,list,dict)):
        return json.dumps(v)

def encode_row(row):
    if isinstance(row, dict):
        return {k: encode_field(v) for k,v in row.items() if v is not None}
    
    elif isinstance(row, (tuple,list)):
        encoded_row = [encode_field(v) for v in row]
        if isinstance(row, tuple):
            encoded_row = tuple(encoded_row)

        return encoded_row

def decode_field(v):
    return v

def decode_row(row):
    if isinstance(row, dict):
        return {k: decode_field(v) for k,v in row.items()}
    
    elif isinstance(row, (tuple,list)):
        decoded_row = [decode_field(v) for v in row]
        if isinstance(row, tuple):
            decoded_row = tuple(decoded_row)

        return decoded_row


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