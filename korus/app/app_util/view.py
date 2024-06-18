import pandas as pd
from korus.util import list_to_str


max_char = 60


def view_table_contents(conn, table_name, ids=None, columns=None):
    """ Print table contents in human-friendly format.

    Args:
        conn: sqlite3.Connection
            Database connection
        table_name: str
            Table name
        ids: int,list(int)
            Row indices to include. If None, all rows will be printed.
        columns: str,list(str)
            Columns to include. If None, all columns will be printed.

    Returns:
        None
    """
    if columns is None:
        columns = []
    elif isinstance(columns, str):
        columns = [columns]

    c = conn.cursor()
    query = f"SELECT * FROM {table_name}"
    if ids is not None:
        ids_str = list_to_str(ids)
        query += f" WHERE id IN {ids_str}"

    data = c.execute(query).fetchall()
    if len(data) == 0:
        print(f"table `{table_name}` is empty")

    else:
        col_names = c.execute(f"SELECT name FROM PRAGMA_TABLE_INFO('{table_name}')").fetchall()
        col_names = [name[0] for name in col_names]
        df = pd.DataFrame(data, columns=col_names)

        # restrict to subset of columns
        if len(columns) > 0:
            df = df[columns]                    

        line = "-" * max_char
        for idx,row in df.iterrows():
            print(line)
            for key,val in row.items():
                if val != "None" and val is not None:
                    if isinstance(val, str) and len(val) > max_char:
                        val = val[:max_char] + "..."

                    print(f"{key}: {val}")


def view_data_storage_locations(conn):
    view_table_contents(conn, table_name="storage")

def view_jobs(conn):
    view_table_contents(conn, table_name="job")

def view_deployments(conn):
    view_table_contents(conn, table_name="deployment")

def view_files(conn, file_ids=None):
    view_table_contents(conn, table_name="file", ids=file_ids)

def view_tags(conn):
    view_table_contents(conn, table_name="tag")

def view_taxonomies(conn):
    view_table_contents(conn, table_name="taxonomy", columns=["id","name","version","timestamp","comment"])
