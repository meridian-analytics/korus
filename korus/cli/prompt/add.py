import inquirer
from korus.database.database import Database
from korus.database.interface import FieldDefinition
import korus.cli.prompt.prompt as prompt
import korus.cli.text as txt
from korus.cli.prompt.view import view_contents_condensed
from korus.cli.cursor import cursor
import korus.cli.prompt.file as fil


def add(db: Database, table_name: str):
    if table_name == "file":
        add_file(db)

    elif table_name == "annotation":
        add_annotation(db)

    else:
        add_row(db, table_name)


def add_file(db: Database, filename: str | list[str] = None) -> list[int]:
    table_name = "file"
    tbl = getattr(db, table_name)

    if filename is None:
        MANUAL = 0
        AUTO = 1

        msg = str(cursor) + "Select method for adding audiofile metadata"
        choices = {
            "Automated, batch  (recommended)": AUTO,
            "Manual, single file": MANUAL,
        }
        choice = inquirer.list_input(msg, choices=choices.keys())
        method = choices[choice]

        if method == MANUAL:
            return add_row(db, table_name)

    # prompt user to select storage location
    field = tbl.fields_asdict["storage_id"]
    msg = "Specify where the audiofiles are stored"
    storage_id = get_field_value(db, table_name, field, msg)

    # prompt user to select timestamp parser
    timestamp_parser = prompt.select_timestamp_parser()

    # storage attrs
    (dir_path, by_date) = db.storage.get(storage_id, fields=["path", "by_date"])[0]

    if filename is None:

        FILE_TXT = 0
        FILE_CSV = 1
        FILE_RAVEN = 2
        FILE_CONSOLE = 3
        FILE_TIME = 4
        FILE_ALL = 5

        msg = (
            str(cursor)
            + "Select method for filtering audio files at the given storage location"
        )
        choices = {
            "Extract filenames from a text file": FILE_TXT,
            "Extract filenames from a CSV file": FILE_CSV,
            "Extract filenames from a RavenPro selection table": FILE_RAVEN,
            "Specify filename(s) in the console": FILE_CONSOLE,
            "Constrain the time range": FILE_TIME,
            "Find all files": FILE_ALL,
        }
        choice = inquirer.list_input(msg, choices=choices.keys())
        method = choices[choice]

        if method == FILE_TXT:
            df = fil.from_txt(dir_path, timestamp_parser)
        elif method == FILE_CSV:
            df = fil.from_csv(dir_path, timestamp_parser)
        elif method == FILE_RAVEN:
            df = fil.from_raven(dir_path, timestamp_parser)
        elif method == FILE_CONSOLE:
            df = fil.from_console(dir_path, timestamp_parser)
        elif method == FILE_TIME:
            df = fil.from_time_range(dir_path, timestamp_parser, by_date)
        elif method == FILE_ALL:
            df = fil.from_filename(dir_path, timestamp_parser)

    else:
        df = fil.from_filename(dir_path, timestamp_parser, filename)

    # prompt user for deployment ID
    msg = (
        "Enter "
        + txt.bold("id")
        + " of the hydrophone deployment that the audiofiles belong to"
    )
    deployment_id = prompt.enter_index(db, "deployment", msg)

    # add files, one at the time
    print(txt.info(f"Adding {len(df)} files to database ..."))
    tbl = getattr(db, table_name)

    print()
    print(df.to_string())

    raise KeyboardInterrupt
    # for _,row in tqdm(df.iterrows(), total=len(df)):
    #    row.update({"deployment_id": deployment_id})
    #    tbl.add(row)


def add_annotation(db: Database) -> list[int]:
    # TODO: implement this
    return add_row(db, "annotation")


def add_deployment(db: Database) -> int:
    # TODO: allow user to select between fixed or mobile
    pass


def get_field_value(db: Database, table_name: str, field: FieldDefinition):
    """Get a value for a given field by prompting the user.

    Args:
        db: korus.database.Database
            The database instance
        table_name: str
            Table name
        field: FieldDefinition
            The field

    Returns:
        value: 
            The inputted value
    """
    cursor.item = field.name

    while True:
        action, kwargs = prompt.select_field_action(db, table_name, field)

        try:
            if action == prompt.FIELD_INFO:
                print(field.info())

            if action == prompt.FIELD_ENTER:
                value = prompt.enter_value(field, **kwargs)
                break

            elif action == prompt.FIELD_SELECT:
                value = prompt.select_value(field, **kwargs)
                break

            elif action == prompt.FIELD_EXTERNAL:
                view_contents_condensed(db, **kwargs)

            elif action == prompt.FIELD_SKIP:
                value = None
                break

        except KeyboardInterrupt:
            continue

    return value


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
        value = get_field_value(db, table_name, field)
        if value is not None:
            row[field.name] = value

    idx = tbl.add(row)

    print(
        txt.info(f"\nSuccessfully added new row with id={idx} to {table_name} table.")
    )

    return idx
