import pandas as pd
import numpy as np
import textwrap
from tabulate import tabulate
from korus.util import list_to_str
import korus.app.app_util.ui as ui


# maximum number of characters per line
MAX_CHAR = 60


def view_table_contents(conn, table_name, ids=None, columns=None, transform_fcn=lambda name,value: value):
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
        transform_fcn: callable
            Function with signature (name, value) -> value for transforming table values

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

        num_items = len(df)
        item_no = 0

        ui_select_item = ui.UserInput(
            "select_table_item", 
            f"Select item (1 - {num_items}), press ENTER to view next item, or Ctrl-c to return to the previous menu.", 
            group=f"view_{table_name}",
            transform_fcn=lambda x: int(x) if len(x) > 0 else 0,
            allowed_values=[i for i in range(0, num_items + 1)],
        )

        while True:
            try:
                # prompt user to specify item no.
                new_item_no = ui_select_item.request()
                if new_item_no == 0:
                    new_item_no = max(1, (item_no + 1) % (num_items + 1))

                item_no = new_item_no

                #collect item data in nested list (format expected by tabulate)
                row = df.iloc[item_no - 1]
                tbl = []
                for k, v in row.to_dict().items():
                    # transform table value before printing
                    v = transform_fcn(k, v)

                    # insert line breaks to respect max line length
                    if isinstance(v, str):
                        v = textwrap.fill(v, MAX_CHAR)

                    tbl.append([k, v])

                # pretty print                
                print(tabulate(tbl, headers=["Field", "Value"], tablefmt='psql'))

            except KeyboardInterrupt:
                break

def view_data_storage_locations(conn):
    view_table_contents(conn, table_name="storage")

def view_jobs(conn):
    def transform_fcn(name, value):
        """ Transform function for converting label IDs to source/type names """
        if name == "primary_sound":
            ids = value.replace("[","").replace("]","").replace(" ","").split(",")
            query = f"SELECT sound_source_tag,sound_type_tag FROM label WHERE id IN {list_to_str(ids)}"
            c = conn.cursor()
            rows = c.execute(query).fetchall()
            return rows

        return value

    view_table_contents(conn, table_name="job", transform_fcn=transform_fcn)

def view_deployments(conn):
    view_table_contents(conn, table_name="deployment")

def view_files(conn, file_ids=None):
    view_table_contents(conn, table_name="file", ids=file_ids)

def view_tags(conn):
    view_table_contents(conn, table_name="tag")

def view_taxonomies(conn):
    view_table_contents(conn, table_name="taxonomy", columns=["id","name","version","timestamp","comment"])
