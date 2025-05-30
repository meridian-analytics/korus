import numpy as np


def get_sqlite_type(x: "typing.Any"):
    if x in [int, bool]:
        return "INTEGER"

    elif x == float:
        return "REAL"

    elif x in [tuple, list, dict]:
        return "JSON"

    else:
        return "TEXT"


def to_str(x):
    """Transform the input to a string, suitably formatted for forming SQLite queries.

    Example query: `SELECT * FROM y WHERE z IN {to_str(x)}`

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

    return "(" + ",".join([f"'{v}'" if isinstance(v, str) else f"{v}" for v in x]) + ")"


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


def get_column_types(conn, table_name):
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {row[1]: row[2] for row in rows}


def get_row_count(conn, table_name):
    col_name = get_column_names(conn, table_name)[0]
    q = f"SELECT count({col_name}) FROM {table_name}"
    n = conn.execute(q).fetchall()[0][0]
    return n


def where_condition(conn, table_name, conditions, invert):
    col_types = get_column_types(conn, table_name)

    # left joins on JSON columns
    left_joins = []
    for condition in conditions:
        for name in condition.keys():
            if col_types[name] == "JSON":
                left_joins.append(
                    f"LEFT JOIN json_each('{table_name}'.'{name}') AS {name}"
                )

    left_joins = " ".join(left_joins)

    # WHERE conditions
    logical_or = []
    for condition in conditions:
        logical_and = []
        for name, values in condition.items():
            x = f"{name}"
            if col_types[name] == "JSON":
                x += ".value"

            if isinstance(values, tuple):
                a, b = values
                if isinstance(a, str):
                    a = f"'{a}'"
                if isinstance(b, str):
                    b = f"'{b}'"

                cond = []
                if invert:
                    if a is not None:
                        cond.append(f"{x} < {a}")
                    if b is not None:
                        cond.append(f"{x} > {b}")
                    cond = " OR ".join(cond)

                else:
                    if a is not None:
                        cond.append(f"{x} >= {a}")
                    if b is not None:
                        cond.append(f"{x} <= {b}")
                    cond = " AND ".join(cond)

            else:
                if invert:
                    cond = f"{x} NOT IN {to_str(values)}"
                else:
                    cond = f"{x} IN {to_str(values)}"

            logical_and.append(cond)

        logical_or.append("(" + " AND ".join(logical_and) + ")")

    if len(logical_or) > 0:
        return left_joins + " WHERE " + " OR ".join(logical_or)

    else:
        return None


def search_table(conn, table_name, condition=None, indices=None):
    c = conn.cursor()

    if indices is not None:
        id_cond = f"WHERE id IN {to_str(indices)}"

        if condition is None:
            condition = id_cond

        else:
            condition = condition.replace("WHERE", id_cond + " AND")

    q = f"SELECT {table_name}.id FROM {table_name} {condition}"
    rows = c.execute(q).fetchall()
    return [row[0] for row in rows]


def fetch_row(
    conn, table_name, indices=None, fields=None, as_dict=False, return_indices=False
):
    c = conn.cursor()

    left_joins = []

    if isinstance(fields, str):
        fields = [fields]

    if fields is None:
        fields_str = "*"

    else:
        fields = ["id"] + fields

        # left joins
        for field in fields:
            if "." in field:
                tbl = field.split(".")[0]
                left_joins.append(
                    f" LEFT JOIN {tbl} ON {table_name}.{tbl}_id = {tbl}.id"
                )

        fields_str = ",".join(fields)

    q = f"SELECT {fields_str} FROM {table_name}"

    for left_join in left_joins:
        q += left_join

    if indices is not None:
        q += f" WHERE id IN {to_str(indices)}"

    rows = c.execute(q).fetchall()

    # remove index, if not requested
    if not return_indices:
        rows = [row[1:] for row in rows]

    # preserve index ordering
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
    values = ", ".join(
        [f"{k} = '{v}'" if isinstance(v, str) else f"{k} = {v}" for k, v in row.items()]
    )
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
