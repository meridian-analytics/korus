import inquirer
from korus.database.database import Database
import korus.cli.prompt.prompt as prompt
import korus.cli.text as txt
from korus.cli.prompt.view import view_contents_condensed
from korus.cli.cursor import cursor
from korus.audio import collect_audiofile_metadata


def add(db: Database, table_name: str):
    if table_name == "file":
        add_file(db)

    elif table_name == "annotation":
        add_annotation(db)

    else:
        add_row(db, table_name)


def add_file(db: Database, filename: str | list[str] = None) -> list[int]:
    table_name = "file"
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

    # deployment
    deployment_id = prompt.enter_index(db, "deployment")

    # storage location
    storage_id = prompt.enter_index(db, "storage")

    # datetime format
    timestamp_parser = prompt.select_timestamp_parser()
    raise KeyboardInterrupt

    # search for files and parse timestamps
    # first, only obtain the timestamps (fast)
    """
    df = collect_audiofile_metadata(
        path=audio_path,
        ext=audio_format,
        timestamp_parser=timestamp_parser,
        earliest_start_utc=start_utc,
        latest_start_utc=end_utc,
        progress_bar=True,
        date_subfolder=date_subfolder,
        inspect_files=False,
    )

    cprint(
        f" ## Found {len(df)} {audio_format} files in the folder {audio_path} between {start_utc} and {end_utc}",
        "yellow",
    )
    """

    """
    [x] select between manual and automated (recommended) ingestion
    [x] select deployment
    [x] select storage location
        TODO: add `date_stamped` field to storage table to indicate if files are organized into date-stamped subfolders
    specify datetime format*
    if filename is None, give user options to
     - inputing filename/file**, or
     - specifying time range
     - search all files
    automatic search for files and parsing of timestamps
    create Checkbox question with all files (and parsed timestamps)
     - check all found files
     - uncheck files that were not found
     - ask user to uncheck any files they dont want added
    automatic extraction of metadata

    * store inputted datetime formats in .korus file?
    * TODO: use https://labix.org/python-dateutil for parsing timestamps
    * there is also: https://github.com/Parquery/datetime-glob does wildcards, but only microseconds

    ** allow for multiple formats, e.g.
      - plain text file with paths/filenames in each line
      - csv/tsv file with lowercase header filename/path/begin file/...
    """

    return add_row(db, "file")


def add_annotation(db: Database) -> list[int]:
    # TODO: implement this
    return add_row(db, "annotation")


def add_deployment(db: Database) -> int:
    # TODO: allow user to select between fixed or mobile
    pass


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

        cursor.item = field.name

        while True:
            action, kwargs = prompt.select_field_action(db, table_name, field)

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
