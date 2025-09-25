import os
import shutil
import inquirer
from tqdm import tqdm
import pandas as pd
from datetime import datetime
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

    elif table_name == "job":
        add_job(db)

    elif table_name == "deployment":
        add_deployment(db)

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

    field = tbl.fields_asdict["deployment_id"]
    msg = "Specify which hydrophone deployment the audiofiles are from"
    deployment_id = get_field_value(db, "deployment", field, msg)

    # add files, one at the time
    print(txt.info(f"Adding {len(df)} files to database ..."))
    tbl = getattr(db, table_name)
    indices = []
    for _, row in tqdm(df.iterrows(), total=len(df)):
        row_dict = row.to_dict()
        row_dict.update({"storage_id": storage_id, "deployment_id": deployment_id})
        idx = tbl.add(row_dict)
        indices.append(idx)

    # let user know everything went well
    msg = txt.info(f"\nSuccessfully added {len(indices)} files to the database.")
    print(msg)

    return indices


def add_job(db: Database) -> int:
    table_name = "job"
    tbl = getattr(db, table_name)

    row = {}
    for field in tbl.fields:
        if field.name == "target":
            continue

        value = get_field_value(db, table_name, field)
        if value is not None:
            row[field.name] = value

        # if job is 'exhaustive' prompt user to specify target
        if (
            "is_exhaustive" in row
            and "taxonomy_id" in row
            and "target" not in row
            and row["is_exhaustive"]
        ):

            cursor.item = "target"
            row["target"] = prompt.enter_label(db, row["taxonomy_id"])

    idx = tbl.add(row)

    print(txt.info(f"Successfully added job with id={idx} to the database."))

    return idx


def add_annotation(db: Database) -> list[int]:
    table_name = "annotation"
    tbl = getattr(db, table_name)

    # prompt user for deployment ID
    msg = (
        "Enter "
        + txt.bold("id")
        + " of the hydrophone deployment that the audiofiles belong to"
    )

    field = tbl.fields_asdict["deployment_id"]
    msg = "Specify which hydrophone deployment the audiofiles are from"
    deployment_id = get_field_value(db, "deployment", field, msg)

    # prompt user to specify path to RavenPro selection table(s)
    msg = "Enter the path(s) to the RavenPro selection table(s)"
    paths = prompt.enter_path(multiple=True, msg=msg)

    # prompt user to specify job_id
    field = tbl.fields_asdict["job_id"]
    msg = "Specify which annotation job generated the annotations"
    job_id = get_field_value(db, table_name, field, msg)

    # prompt user to specify granularity_id
    field = tbl.fields_asdict["granularity_id"]
    msg = "Specify the granularity level of the annotations *not* marked as `Batch`"
    granularity_id = get_field_value(db, table_name, field, msg)

    # taxonomy version
    (tax_id,) = db.job.get(job_id, fields="taxonomy_id")[0]

    # granularity level
    (granularity,) = db.granularity.get(granularity_id, fields="name")[0]

    # create folder for storing validated selection tables
    epoch = int(datetime.now().timestamp())
    dir_path = os.path.join(os.getcwd(), f"korus-validation-{epoch}")
    os.makedirs(dir_path)

    # load all tables
    print(txt.info(f"Loading and validating annotations ..."))
    dfs = []
    num_err = []
    for path in paths:
        df, df_raven = tbl.load_raven(
            path=path,
            deployment_id=deployment_id,
            granularity=granularity,
            taxonomy_version=tax_id,
            progress_bar=True,
        )

        dfs.append(df)

        # save validation result
        fname = os.path.basename(path)
        p = fname.rfind(".")
        fname = fname[:p] + "-val" + fname[p:]
        df_raven.to_csv(os.path.join(dir_path, fname), index=False)

        # count errors
        num_err.append({"filename": fname, "errors": (~df_raven.Valid).sum()})

    # save error counts
    num_err = pd.DataFrame(num_err)
    num_err.to_csv(os.path.join(dir_path, "errors.csv"), index=False)

    num_err_tot = num_err.errors.sum()

    if num_err_tot == 0:
        # if there are no errors, inform user and remove tmp directory
        print(txt.info(f"Validation successful. No errors detected."))
        shutil.rmtree(dir_path)

    else:
        # if errors were found, alert user and exit
        msg = (
            f"Validation failed. Errors detected in {num_err_tot} entries out of {len(df)}."
            + f" The validated tables have been saved to {dir_path}."
            + f" Please review and correct the offending entries before resubmitting."
        )
        print(txt.error(msg))
        raise KeyboardInterrupt

    # concat
    df = pd.concat(dfs, ignore_index=True)

    # add annotations
    print(txt.info(f"Adding {len(df)} annotations to database ..."))
    indices = tbl.add_batch(df)

    # let user know everything went well
    msg = txt.info(f"\nSuccessfully added {len(indices)} annotations to the database.")
    print(msg)

    return indices


def add_deployment(db: Database) -> int:
    # TODO: allow user to select between fixed or mobile
    # TODO: implement this
    return add_row(db, "deployment")


def get_field_value(
    db: Database,
    table_name: str,
    field: FieldDefinition,
    msg: str = "Select field action",
):
    """Get a value for a given field by prompting the user.

    Args:
        db: korus.database.Database
            The database instance
        table_name: str
            Table name
        field: FieldDefinition
            The field
        msg: str
            Prompt message

    Returns:
        value:
            The inputted value
    """
    cursor.item = field.name

    while True:
        action, kwargs = prompt.select_field_action(db, table_name, field, msg)

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
