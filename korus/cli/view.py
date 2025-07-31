import inquirer
from korus.database.database import Database
from korus.database.interface import TableViewer


def view_contents_detailed(db: Database, table_name: str):
    """View table contents in detail

    Args:
        db: korus.database.Database
            The database instance
        table_name: str
            Table name
    """
    tbl = getattr(db, table_name)
    viewer = TableViewer(tbl, nrows=1)
    counter = 0
    for page in iter(viewer):
        if counter >= 1:
            proceed = inquirer.confirm("Continue?", default=True)
            if not proceed:
                break

        print(page)
        counter += 1


def view_contents_condensed(db: Database, table_name: str, required: bool = True):
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
    if required:
        field_names = [field.name for field in tbl.fields if field.required]
    else:
        field_names = None

    viewer = TableViewer(tbl, field_names)
    counter = 0
    for page in iter(viewer):
        if counter >= 1:
            proceed = inquirer.confirm("Continue?", default=True)
            if not proceed:
                break

        print(page)
        counter += 1
