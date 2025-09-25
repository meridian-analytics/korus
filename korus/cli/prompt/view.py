import inquirer
from korus.database.database import Database
from korus.database.interface import TableViewer
import korus.cli.text as txt


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

    if table_name == "taxonomy":
        field_names = ["version", "timestamp", "changes", "comment"]
        transforms = {
            "timestamp": lambda x: x.strftime("%Y-%m-%d %H:%M:%S"),
            "changes": lambda x: None if x is None else " | ".join(x),
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

    if table_name == "taxonomy":
        field_names = ["version", "timestamp", "changes", "comment"]
        transforms = {
            "timestamp": lambda x: x.strftime("%Y-%m-%d %H:%M:%S"),
            "changes": lambda x: None if x is None else " | ".join(x),
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
