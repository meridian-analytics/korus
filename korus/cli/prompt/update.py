from korus.database.database import Database
import korus.cli.prompt as prompt
import korus.cli.parse as parse
import korus.cli.text as txt


def update(db: Database, table_name: str):
    if table_name == "job":
        update_job(db)

    else:
        update_field(db, table_name)


def update_job(db: Database):
    # TODO: implement this; allow user to link files to job
    return update_field(db, "job")


def update_field(db: Database, table_name: str):
    """Update a field in the specified table.

    Args:
        db: korus.database.Database
            The database instance
        table_name: str
            Table name

    Raises:
        KeyboardInterrupt: if the user hits Ctrl+C or the attempt to update the row fails
    """
    idx = prompt.enter_index(db, table_name)
    field = prompt.select_field(db, table_name)
    value = prompt.enter_value(table_name, field)
    if value is None:
        return

    value = parse.parse_value(field, value)
    row = {field.name: value}
    tbl = getattr(db, table_name)

    try:
        tbl.update(idx, row)
        print(txt.info(f"Successfully update row with id={idx} in {table_name} table."))

    except Exception as err:
        msg = f"Failed to update row in {table_name} table. " + str(err)
        print(txt.error(msg))
        raise KeyboardInterrupt
