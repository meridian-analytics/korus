import inquirer
import pandas as pd
from korus.database.database import Database
from korus.database.interface import TableViewer
import korus.cli.text as txt
from korus.cli.cursor import cursor


def label_as_str(label: tuple | list[tuple]) -> str:
    """Helper function for encoding (source,type) labels as strings"""
    if isinstance(label, tuple):
        label = [label]

    if label is None:
        return ""

    else:
        label = [f"{x[0]},{x[1]}" for x in label]
        return "; ".join(label)


def view_info(db: Database, table_name: str):
    """View table information

    Args:
        db: korus.database.Database
            The database instance
        table_name: str
            Table name
    """
    tbl = getattr(db, table_name)
    print(tbl)


def view_contents(db: Database, table_name: str):
    """View table contents

    Args:
        db: korus.database.Database
            The database instance
        table_name: str
            Table name
    """
    tbl = getattr(db, table_name)

    if len(tbl) == 0:
        print(txt.info(f"The {table_name} table is empty"))
        return

    # for each table, specify default field choices and transforms
    if table_name == "taxonomy":
        defaults = ["version", "timestamp", "changes", "comment"]
        transforms = {
            "timestamp": lambda x: x.strftime("%Y-%m-%d %H:%M:%S"),
            "changes": lambda x: None if x is None else " | ".join(x),
        }

    elif table_name == "file":
        defaults = tbl.field_names
        transforms = {
            "start_utc": lambda x: (
                "" if x is None or pd.isna(x) else x.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            ),
            "end_utc": lambda x: (
                "" if x is None or pd.isna(x) else x.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            ),
        }        

    elif table_name == "job":
        defaults = [
            "annotator",
            "completion_date",
            "is_exhaustive",
            "target",
            "taxonomy_id",
            "comments",
        ]
        transforms = {
            "completion_date": lambda x: "" if x is None or pd.isna(x) else x.strftime("%Y-%m-%d"),
        }

    elif table_name == "annotation":
        defaults = [
            "deployment_id",
            "start_utc",
            "duration",
            "label",
            "tentative_label",
            "ambiguous_label",
            "multiple_label",
            "excluded_label",
            "tag",
            "comments",
        ]
        transforms = {
            "start_utc": lambda x: (
                "" if x is None or pd.isna(x)  else x.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            ),
            "label": label_as_str,
            "tentative_label": label_as_str,
            "ambiguous_label": label_as_str,
            "multiple_label": label_as_str,
            "excluded_label": label_as_str,
        }

    else:
        # for all other tables, include all fields
        defaults = tbl.field_names
        transforms = None

    # prompt user to select which fields to display
    msg = (
        str(cursor)
        + "Select the fields you wish to display"
    )
    field_names = inquirer.checkbox(
        msg,
        choices=tbl.field_names + tbl.alias_names,
        default=defaults,
    )

    viewer = TableViewer(tbl, field_names, transforms=transforms)
    counter = 0
    for page in iter(viewer):
        if counter >= 1:
            proceed = inquirer.confirm("Continue?", default=True)
            if not proceed:
                break

        print(page)
        counter += 1
