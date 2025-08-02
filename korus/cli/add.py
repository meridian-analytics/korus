from korus.database.database import Database
import korus.cli.prompt as prompt
import korus.cli.parse as parse
import korus.cli.text as txt
from .view import view_contents_condensed


def add(db: Database, table_name: str):
    if table_name == "file":
        add_file(db)

    elif table_name == "annotation":
        add_annotation(db)

    else:
        add_row(db, table_name)


def add_file(db: Database, filename: str | list[str] = None) -> list[int]:
    # TODO: implement this

    """
    select between manual and automated (recommended) ingestion
    select deployment
    select storage location
        TODO: add `date_stamped` field to storage table to indicate if files are organized into date-stamped subfolders
    specify datetime format*
    if filename is None, give user options to
     - inputing filename/file, or
     - specifying time range
     - search all files
    automatic search for files and parsing of timestamps
    create Checkbox question with all files
     - check all found files
     - uncheck files that were not found
     - ask user to uncheck any files they dont want added
    automatic extraction of metadata

    * store inputted datetime formats in .korus file?
    * TODO: use https://labix.org/python-dateutil for parsing timestamps!
    """

    return add_row(db, "file")


def add_annotation(db: Database) -> list[int]:
    # TODO: implement this
    return add_row(db, "annotation")


def add_row(db: Database, table_name: str) -> int:
    """Add a single row to the specified table.

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
                    view_contents_condensed(db, **kwargs)

                elif action == prompt.FIELD_SKIP:
                    value = None
                    break

            except KeyboardInterrupt:
                continue

        if value is not None:
            row[field.name] = value

    idx = tbl.add(row)

    print(txt.info(f"Successfully added new row with id={idx} to {table_name} table."))

    return idx
