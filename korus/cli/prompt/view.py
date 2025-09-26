import inquirer
from korus.database.database import Database
from korus.database.interface import TableViewer
import korus.cli.text as txt


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


def view_contents_detailed(db: Database, table_name: str):
    """View table contents in detail

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

    # for each table, specify which fields should be printed
    if table_name == "taxonomy":
        field_names = ["version", "timestamp", "changes", "comment"]
        transforms = {
            "timestamp": lambda x: x.strftime("%Y-%m-%d %H:%M:%S"),
            "changes": lambda x: None if x is None else " | ".join(x),
        }

    elif table_name == "job":
        field_names = [
            "annotator",
            "end_utc",
            "is_exhaustive",
            "target",
            "taxonomy_id",
            "comments",
        ]
        transforms = {
            "end_utc": lambda x: "" if x is None else x.strftime("%Y-%m-%d"),
        }

    elif table_name == "annotation":
        field_names = [
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
                "" if x is None else x.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            ),
            "label": label_as_str,
            "tentative_label": label_as_str,
            "ambiguous_label": label_as_str,
            "multiple_label": label_as_str,
            "excluded_label": label_as_str,
        }

    else:
        field_names = None
        transforms = None

    viewer = TableViewer(tbl, field_names, nrows=1, transforms=transforms)
    counter = 0
    for page in iter(viewer):
        if counter >= 1:
            proceed = inquirer.confirm("Continue?", default=True)
            if not proceed:
                break

        print(page)
        counter += 1


def view_contents_condensed(db: Database, table_name: str):
    """View table contents in condensed form

    Args:
        db: korus.database.Database
            The database instance
        table_name: str
            Table name
        required: bool
            Only show required fields.
    """
    tbl = getattr(db, table_name)

    if len(tbl) == 0:
        print(txt.info(f"The {table_name} table is empty"))
        return

    # for each table, specify which fields should be printed
    if table_name == "taxonomy":
        field_names = ["version", "timestamp", "changes", "comment"]
        transforms = {
            "timestamp": lambda x: x.strftime("%Y-%m-%d %H:%M:%S"),
            "changes": lambda x: None if x is None else " | ".join(x),
        }

    elif table_name == "job":
        field_names = ["annotator", "end_utc", "is_exhaustive", "target", "taxonomy_id"]
        transforms = {
            "end_utc": lambda x: "" if x is None else x.strftime("%Y-%m-%d"),
        }

    elif table_name == "annotation":
        field_names = ["start_utc", "duration", "label"]
        transforms = {
            "start_utc": lambda x: (
                "" if x is None else x.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            ),
            "label": label_as_str,
        }

    else:
        # for all other tables, include all `required` fields
        field_names = [field.name for field in tbl.fields if field.required]
        transforms = None

    viewer = TableViewer(tbl, field_names, transforms=transforms)
    counter = 0
    for page in iter(viewer):
        if counter >= 1:
            proceed = inquirer.confirm("Continue?", default=True)
            if not proceed:
                break

        print(page)
        counter += 1
