import inquirer
from tabulate import tabulate
from korus.database.database import Database
from korus.database.interface import TableViewer
import korus.cli.prompt as prompt
import korus.cli.parse as parse


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


def add_row(db: Database, table_name: str) -> int:
    """Add a row to the specified table.

    Args:
        db: korus.database.Database
            The database instance
        table_name: str
            Table name

    Returns:
        : int
            The index assigned to the added row

    Raises:
        KeyboardInterrupt: if the user hits Ctrl+C
    """
    tbl = getattr(db, table_name)

    row = {}

    for field in tbl.fields:

        while True:
            action, kwargs = prompt.field_action(db, table_name, field)

            try:
                if action == prompt.FIELD_INFO:
                    print(field.info())

                if action == prompt.FIELD_ENTER:
                    value = prompt.enter_value(table_name, field, **kwargs)
                    break

                elif action == prompt.FIELD_SELECT:
                    value = prompt.select_value(table_name, field, **kwargs)
                    break

                elif action == prompt.FIELD_EXTERNAL:
                    view_contents(db, **kwargs)

                elif action == prompt.FIELD_SKIP:
                    value = None
                    break

            except KeyboardInterrupt:
                continue

        if value is not None:
            row[field.name] = parse.parse_value(field, value)

    return tbl.add(row)
