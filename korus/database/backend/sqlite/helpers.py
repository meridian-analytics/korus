import numpy as np
import sqlite3
from korus.database.backend import TableBackend
from korus.database.backend.sqlite.encode import Codec, get_sqlite_type, encode_key


class SQLiteTableBackend(TableBackend):
    """Generic SQLite table backend.

    Args:
        conn: sqlite3.Connection
            The database connection
        name: str
            The table name
        codec: korus.database.backend.sqlite.encode.Codec
            Encoder-decoder for inserting and retrieving data from the database
    """

    def __init__(self, conn: sqlite3.Connection, name: str, codec: Codec):
        super().__init__(name)
        self.conn = conn
        self.codec = codec

    def __len__(self):
        return get_row_count(self.conn, self.name)

    def add(self, row: dict):
        insert_row(self.conn, self.name, self.codec.encode(row, self.name))
        self.conn.commit()

    def set(self, idx: int, row: dict):
        update_row(
            self.conn, self.name, encode_key(idx), self.codec.encode(row, self.name)
        )
        self.conn.commit()

    def get(
        self,
        indices: int | list[int] = None,
        fields: str | list[str] = None,
        return_indices: bool = False,
    ):
        rows = fetch_row(
            self.conn,
            self.name,
            encode_key(indices),
            fields,
            as_dict=True,
            return_indices=return_indices,
        )
        rows = [self.codec.decode(row, self.name) for row in rows]
        return [tuple(list(row.values())) for row in rows]

    def add_field(
        self,
        name: str,
        type: "typing.Any",
        default: "typing.Any" = None,
        required: bool = True,
    ):
        sqlite_type = get_sqlite_type(type)
        sqlite_default = self.codec.encode(default, self.name, name)

        add_column(
            self.conn,
            self.name,
            name,
            sqlite_type,
            required,
            sqlite_default,
        )

        self.conn.commit()


def to_str(x):
    """Transform the input to a string, suitably formatted for forming SQLite queries.

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


def table_exists(conn, name):
    """Check if the database already has a table with a given name

    Args:
        conn: sqlite3.Connection
            Database connection
        name: str
            Table name

    Returns:
        : bool
            True, if table exists. False, otherwise.
    """
    return name in get_table_names(conn)


def get_table_names(conn):
    c = conn.cursor()
    query = f"""
        SELECT 
            name 
        FROM 
            sqlite_master 
        WHERE 
            type='table' 
    """
    rows = c.execute(query).fetchall()
    return [row[0] for row in rows]


def get_column_names(conn, table_name):
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [row[1] for row in rows]


def get_row_count(conn, table_name):
    col_name = get_column_names(conn, table_name)[0]
    q = f"SELECT count({col_name}) FROM {table_name}"
    n = conn.execute(q).fetchall()[0][0]
    return n


def fetch_row(
    conn, table_name, indices=None, fields=None, as_dict=False, return_indices=False
):
    c = conn.cursor()

    if isinstance(fields, str):
        fields = [fields]

    if fields is None:
        fields_str = "*"

    else:
        fields = ["id"] + fields
        fields_str = ",".join(fields)

    q = f"SELECT {fields_str} FROM {table_name}"

    if indices is not None:
        q += f" WHERE id IN {to_str(indices)}"

    rows = c.execute(q).fetchall()

    # remove index, if not requested
    if not return_indices:
        rows = [row[1:] for row in rows]

    # preserve ordering
    if indices is not None and np.ndim(indices) > 0:
        idx = np.argsort(np.argsort(indices))
        rows = [rows[i] for i in idx]

    if as_dict:
        if fields is None:
            fields = get_column_names(conn, table_name)

        if not return_indices:
            fields.remove("id")

        rows = [{k: v for k, v in zip(fields, row)} for row in rows]

    return rows


def insert_row(conn, table_name, row):
    """Insert a row of values into a table in the database.

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

    col_names = ",".join(row.keys())
    val_str = ",".join(["?" for _ in row.keys()])
    if has_id(conn, table_name):
        col_names = "id," + col_names
        val_str = "NULL," + val_str

    c.execute(
        f"INSERT INTO {table_name} ({col_names}) VALUES ({val_str})", list(row.values())
    )

    return c


def update_row(conn, table_name, idx, row):
    """Update a row in a table in the database.

    Note: Expects the table to have an `id` index column

    Args:
        conn: sqlite3.Connection
            Database connection
        table_name: str
            Table name
        idx: int
            Identifier of the row to be replaced
        row: dict
            row to be inserted

    Returns:
        c: sqlite3.Cursor
            Database cursor
    """
    assert_msg = (
        f"Unable to update table {table_name} because it does not have an `id` column"
    )
    assert has_id(conn, table_name), assert_msg

    c = conn.cursor()
    values = ", ".join([f"{k} = {v}" for k, v in row.items()])
    q = f"UPDATE {table_name} SET {values} WHERE id = {idx}"
    c.execute(q)
    return c


def has_id(conn, table_name):
    columns = conn.execute(f"PRAGMA table_info({table_name})")
    for col in columns:
        if col[1] == "id":
            return True

    return False


def add_column(
    conn, table_name, col_name, col_type, required=False, default_value=None
):
    q = f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}"

    if required:
        q += " NOT NULL"

    if default_value is not None:
        q += f" DEFAULT '{default_value}'"

    conn.execute(q)
