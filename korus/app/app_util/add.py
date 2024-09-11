import os
import sqlite3
import json
import yaml
import pandas as pd
import numpy as np
from glob import glob
from datetime import datetime, timedelta
from termcolor import colored, cprint
import traceback
from tabulate import tabulate
from korus.util import collect_audiofile_metadata
import korus.db as kdb
from korus.util import list_to_str
import korus.app.app_util.view as vw
import korus.app.app_util.ui as ui


def terminate(conn):
    """ Helper function for gracefully terminating the program"""
    save_changes = ui.UserInputYesNo(
        "save_changes", 
        "Save changes made to the local Korus database? [y/N]", 
    ).request()

    if save_changes:
        conn.commit()
        cprint(f"\n ## Saved changes to the database", "yellow")

    cprint("\n ## Closing database connection and exiting ...", "yellow")
    conn.close()
    exit(1)


def add_deployment(conn, logger):
    """ Interactive session for adding a new deployment to the database.

        Args:
            conn: sqlite3.Connection
                Database connection
            logger: korus.app.app_util.ui.InputLogger
                Input logger

        Returns:
            : int
                Row index of the new entry
    """
    cprint(f"\n ## Collecting metadata about the deployment", "yellow")

    # query user for input
    owner = ui.UserInput("owner", "Data owner/collector", group="deployment").request(logger)
    name = ui.UserInput("name", "Descriptive name for the deployment", group="deployment").request(logger)
    start_utc = ui.UserInput("start_utc", "UTC start date or time (e.g. 2020-06-01)", group="deployment").request(logger) 
    end_utc = ui.UserInput("end_utc", "UTC end date or time (e.g. 2020-06-01)", group="deployment").request(logger) 
    location = ui.UserInput("location", "Geographic location (e.g. Salish Sea, BC, Canada)", group="deployment").request(logger)
    latitude_deg = ui.UserInput("latitude_deg", "Latitude (degrees)", transform_fcn=float, group="deployment").request(logger)
    longitude_deg = ui.UserInput("longitude_deg", "Longitude (degrees)", transform_fcn=float, group="deployment").request(logger)
    depth_m = ui.UserInput("depth_m", "Depth (meters)", transform_fcn=float, group="deployment").request(logger)
    sample_rate = ui.UserInput("sample_rate", "Sampling rate in samples/s per channel", transform_fcn=int, group="deployment").request(logger)
    data_license = ui.UserInput("data_license", "License/terms for access to and use of data", group="deployment").request(logger)
    hydrophone = ui.UserInput("hydrophone", "Hydrophone make and model", group="deployment").request(logger)
    bits_per_sample = ui.UserInput("bits_per_sample", "Bits per sample", transform_fcn=int, group="deployment").request(logger)
    num_channels = ui.UserInput("num_channels", "Number of recording channels", transform_fcn=int, group="deployment").request(logger)
    sensitivity = ui.UserInput("sensitivity", "Hydrophone sensitivitiy in dB re 1 V/muPa", transform_fcn=float, group="deployment").request(logger)
    comments = ui.UserInput("comments", "Any additional observations", group="deployment").request(logger)

    # collect the data in a dictionary
    data = {
        "owner": owner,  # owner of the data, can be a person or an organisation
        "name": name,  # descriptive name for the deployment
        "start_utc": start_utc, # UTC start time of recording
        "end_utc": end_utc, # UTC end time of recording
        "location": location, # descriptive name for the deployment location
        "latitude_deg": latitude_deg, # latitude, in degrees
        "longitude_deg": longitude_deg, # longitude, in degrees
        "depth_m": depth_m, # depth, in meters
        "license": data_license, # license/terms governing access to and use of data
        "hydrophone": hydrophone, # hydrophone make and model
        "bits_per_sample": bits_per_sample, # bits per sample
        "sample_rate": sample_rate, # sampling rate in samples/s
        "num_channels": num_channels, # no. channels
        "sensitivity": sensitivity, # calibrated sensitivity in ...
        "comments": comments # additional comments
    }

    # print parsed user input
    cprint("\n ## User input:", "yellow")
    for k,v in data.items():
        cprint(f"    * {k}: {v}", "yellow")

    # submit it to the database
    cursor = kdb.insert_row(conn, table_name="deployment", values=data)

    # commit changes
    #conn.commit()

    cprint(f"\n ## Successfully added deployment `{name}` to the database", "yellow")

    return cursor.lastrowid


def add_job(conn, logger):
    """ Interactive session for adding a new annotation job to the database.

        TODO: remove background_sound

        Args:
            conn: sqlite3.Connection
                Database connection
            logger: korus.app.app_util.ui.InputLogger
                Input logger

        Returns:
            : int
                Row index of the new entry
    """
    cprint(f"\n ## Collecting metadata about the annotation job", "yellow")

    default_prim = [
        ("Cetacean","TC"),
        ("KW","PC"),
        ("KW","W"),
    ]

    # query user for input

    ui_taxonomy_id = ui.UserInput("taxonomy_id", "Taxonomy ID (1,2,3,...)", group="job")
    ui_taxonomy_id.add_option(
        key=["v","view"],
        message="View existing taxonomies",
        fcn=lambda x: vw.view_taxonomies(conn)
    )
    taxonomy_id = ui_taxonomy_id.request(logger)

    annotator = ui.UserInput("annotator", "Annotator (initials only)", group="job").request(logger)
    start_utc = ui.UserInput("start_utc", "UTC start date (e.g. 2020-06-01)", group="job").request(logger) 
    end_utc = ui.UserInput("end_utc", "UTC end date (e.g. 2020-06-01)", group="job").request(logger)
    is_exhaustive = ui.UserInputYesNo("is_exhaustive", "Were all primary sounds annotated? [y/N]", group="job").request(logger)

    if is_exhaustive:
        primary_sound = ui.UserInputSound(
            name="primary_sound", 
            message='Primary sounds that were systematically annotated, e.g. [(\'HW\',\'Upsweep\'), (\'HW\',\'Moan\')]',
            conn=conn,
            taxonomy_id=taxonomy_id,
            group="job",
            default=default_prim,
        ).request(logger)
    
    else:
        primary_sound = None

    """
    background_sound = ui.UserInputSound(
        name="background_sound", 
        message='Background sounds that were only opportunistically annotated, e.g., (\'Anthro\',\'%\')',
        conn=conn,
        taxonomy_id=taxonomy_id,
        group="job",
        default=default_backgr,
    ).request(logger)
    """
    background_sound = None

    # prompt user for issues
    ui_add_another_issue = ui.UserInputYesNo(
        "add_another_issue", 
        "Do the annotations have any known issues? [y/N]", 
    )

    issues = []
    while True:
        if ui_add_another_issue.request():
            description = ui.UserInput("add_issue", "Briefly describe the issue").request(logger)
            issues.append(description)

            ui_add_another_issue.message = "Do the annotations have any other known issues? [y/N]"
        else:
            break


    def fcn(x):
        """Helper function for transforming user input for `comments`"""
        # user provides the path to a plain text file with the comments
        if os.path.exists(x):
            with open(x, "r") as f:
                return f.read()

        # user types in the comments in the console
        else:
            return x

    comments = ui.UserInput(
        "comments", 
        "Any additional observations (type comments in console, or provide path to a plain text file)", 
        group="job",
        transform_fcn=fcn,
    ).request(logger)

    # collect in dict
    data = {
        "taxonomy_id": taxonomy_id,
        "annotator": annotator,
        "is_exhaustive": is_exhaustive,
        "start_utc": start_utc,
        "end_utc": end_utc,
        "issues": json.dumps(issues),
        "comments": comments
    }
    if primary_sound is not None and primary_sound != "":
        data["primary_sound"] = primary_sound
    if background_sound is not None and background_sound != "":
        data["background_sound"] = background_sound

    # print parsed user input
    cprint("\n ## User input:", "yellow")
    for k,v in data.items():
        cprint(f"    * {k}: {v}", "yellow")

    # insert into the database
    cursor = kdb.insert_job(conn, values=data)

    # commit changes
    #conn.commit()

    cprint(f"\n ## Successfully added annotation job to the database", "yellow")

    return cursor.lastrowid


def add_data_storage_location(conn, logger):
    """ Interactive session for adding a new data storage location to the database.

        Args:
            conn: sqlite3.Connection
                Database connection
            logger: korus.app.app_util.ui.InputLogger
                Input logger

        Returns:
            : int
                Row index of the new entry
    """
    cprint(f"\n ## Collecting metadata about the data storage location", "yellow")

    def validate_path(x):
        if not os.path.exists(x):
            raise FileNotFoundError
        return x

    name = ui.UserInput(
        "name", 
        "Descriptive name for the data storage location", 
        group="storage",
    ).request(logger)

    audio_path = ui.UserInput(
        "path", 
        "Full path to the data folder",
        group="storage",
        transform_fcn=validate_path,
    ).request(logger)

    description = ui.UserInput(
        "description", 
        "Provide a highly condensed description of the contents of the data folder",
        group="storage",
    ).request(logger)

    # insert into the database
    v = {
        "name": name,
        "path": audio_path,
        "description": description,        
    }
    cursor = kdb.insert_row(conn, table_name="storage", values=v)

    # commit changes
    #conn.commit()

    cprint(f"\n ## Successfully added data storage location to the database", "yellow")

    return cursor.lastrowid


def create_timestamp_parser(group=None, logger=None):
    """ Interative session for creating a timestamp parser 
    
        Args:
            group: str
                Group that the parameter belongs to. Optional. 
                Parameter names must be unique within groups.
            logger: korus.app.app_util.ui.InputLogger
                Input logger

        Returns:
            timestamp_parser: callable
                Takes a string as input and returns a datetime object
    """
    ui_ts_fmt = ui.UserInput(
        "timestamp_format", 
        "Timestamp format, e.g., %Y%m%dT%H%M%S.%f", 
        group=group,
    )
    ui_ts_fmt.add_option(
        key=["s","smru"],
        message="SMRU timestamp format",
        fcn=lambda x: "%Y%m%d_%H%M%S_%f"
    )    
    ts_fmt = ui_ts_fmt.request(logger) 

    ts_offset = ui.UserInput(
        "timestamp_offset", 
        "Time zone offset (hours) relative to UTC", 
        group=group,
        transform_fcn=float,
    ).request(logger)

    ui_ts_siz = ui.UserInput(
        "timestamp_size", 
        "Timestamp length (no. characters)", 
        group=group,
        transform_fcn=int,
    )
    ui_ts_siz.add_option(
        key=["s","smru"],
        message="SMRU timestamp format",
        fcn=lambda x: 19
    )
    ts_siz = ui_ts_siz.request(logger)

    ts_pos = ui.UserInput(
        "timestamp_position", 
        "Timestamp position (no. characters from left if positive / from right if negative)", 
        group=group,
        transform_fcn=int,
    ).request(logger)

    def timestamp_parser(x):        
        p = ts_pos if ts_pos >= 0 else len(x) + ts_pos - ts_siz
        x = x[p : p + ts_siz]
        dt = datetime.strptime(x, ts_fmt)
        dt -= timedelta(seconds=int(ts_offset*3600))
        return dt
    
    return timestamp_parser


def add_files(conn, deployment_id, start_utc, end_utc, logger):
    """ Interactive session for adding a new audio files to the database.

        Args:
            conn: sqlite3.Connection
                Database connection
            deployment_id: int
                Deployment index
            start_utc, end_utc: datetime.datetime
                UTC time range
            logger: korus.app.app_util.ui.InputLogger
                Input logger

        Returns:
            : int
                Row index of the last added audio file
    """

    # query user for data-storage location

    def transform_fcn(x):
        id = int(x)
        c = conn.cursor()
        ids = [row[0] for row in c.execute("SELECT id FROM storage").fetchall()]
        if id not in ids:
            raise ValueError(f"Database does not contain data storage location with ID {id}")
        
        return id

    x = ui.UserInput(
        group="storage",
        name="id",
        message="Enter data storage location ID (1,2,3,...) or add a new data storage location",
        transform_fcn=transform_fcn,
    )
    x.add_option(
        key=["v","view"],
        message="View existing data storage locations",
        fcn=lambda x: vw.view_data_storage_locations(conn)
    )
    x.add_option(
        key=["n","new"],
        message="New data storage location",
        fcn=lambda x: add_data_storage_location(conn, logger)
    )

    try:
        storage_id = x.request(logger)

    except KeyboardInterrupt:
        terminate(conn)


    # query user for further information about the audio files

    (audio_path,) = conn.cursor().execute(f"SELECT path FROM storage WHERE id = {storage_id}").fetchall()[0]

    audio_format = ui.UserInput(
        "audio_format", 
        "Audio file format", 
        group="audio",
    ).request(logger)

    # timestamp parser
    timestamp_parser = create_timestamp_parser("audio", logger)

    date_subfolder = ui.UserInputYesNo(
        "date_subfolder", 
        "Are audio files organized in date-stamped subfolders named `yyymmdd`? [y/N]", 
    ).request(logger)

    # automatically search for audio files and collect metadata

    cprint(f" ## Scanning for {audio_format} files ... (this may take a while)", "yellow")

    # first, only obtain the timestamps (fast)
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

    cprint(f" ## Found {len(df)} {audio_format} files in the folder {audio_path} between {start_utc} and {end_utc}", "yellow")

    if len(df) == 0:
        return 0

    def fcn(x):
        """Helper function for transforming user input for select_files option"""
        # user provides the path to a plain text file with one filename per row
        if os.path.exists(x):
            with open(x, "r") as f:
                lines = f.readlines()
                return [os.path.basename(line.strip()) for line in lines]

        # user provides a list of file IDs or filenames
        else:
            return [y if "." in y else int(y) for y in x.split(",")]
        
    ui_select_files = ui.UserInput(
        "select_files", 
        "Select the files that you want to add to the database, e.g., 0,3,16 or file1.flac,file2.flac or filenames.txt", 
        transform_fcn=fcn,
        group="audio",
    )

    def view_files_fcn():
        for i,(idx,row) in enumerate(df.iterrows()): 
            print(i, row.to_dict())

    ui_select_files.add_option(
        key=["v","view"],
        message="View files",
        fcn=lambda x: view_files_fcn(),
    )

    ui_select_files.add_option(
        key=["a","all"],
        message="Select all files",
        fcn=lambda x: [i for i in range(df.shape[0])],
    )

    file_ids = ui_select_files.request()

    if isinstance(file_ids[0], int):
        df = df.iloc[file_ids]
    else:
        df = df[df.filename.isin(file_ids)]

    # now, inspect the selected files (slow) to also obtain file size and sampling rate
    rel_path = df.apply(lambda r: os.path.join(r.relative_path, r.filename), axis=1)
    df = collect_audiofile_metadata(
        path=audio_path, 
        ext=audio_format, 
        timestamp_parser=timestamp_parser,
        subset=rel_path.values,
        progress_bar=True,
    )

    # insert files into database, one by one
    for _,row in df.iterrows(): 
        data = row.to_dict() #convert pd.Series to dict

        # additional required metadata
        data["storage_id"] = storage_id
        data["deployment_id"] = deployment_id

        # insert in the 'file' table
        try:
            cursor = kdb.insert_row(conn, table_name="file", values=data)

        # to ensure backward compatibility with Korus versions <= 0.0.2
        except sqlite3.IntegrityError:
            data["dir_path"] = data["relative_path"]
            data["location"] = audio_path
            cursor = kdb.insert_row(conn, table_name="file", values=data)

    # commit changes
    #conn.commit()

    cprint(f"\n ## Successfully added {len(df)} audio files to the database", "yellow")

    return cursor.lastrowid, timestamp_parser


def add_annotations(conn, deployment_id, job_id, logger, timestamp_parser=None):
    """ Interactive session for adding new annotations to the database.

        Args:
            conn: sqlite3.Connection
                Database connection
            deployment_id: int
                Deployment index
            job_id: int
                Annotation job index
            logger: korus.app.app_util.ui.InputLogger
                Input logger
            timestamp_parser: callable
                Given a filename (str) returns the UTC timestamp embedded 
                in the filename as a datetime object.
                Only required if some of the annotations reference audio files not present in the database.

        Returns:
            None
    """

    # query user
    path = ui.UserInput(
        "path", 
        "Full path to RavenPro selection table. (Use wildcard to select multiple files.)", 
        group="annotation"
    ).request(logger)

    # get all file IDs
    c = conn.cursor()
    query = f"SELECT id,filename FROM file WHERE deployment_id = '{deployment_id}'"
    rows = c.execute(query).fetchall()
    file_id_map = {row[1]: row[0] for row in rows}  # filename -> file ID mapping

    # load taxonomy
    c = conn.cursor()
    tax_id = c.execute(f"SELECT taxonomy_id FROM job WHERE id = '{job_id}'").fetchall()[0][0]
    tax = kdb.get_taxonomy(conn, tax_id)

    annot_ids = []
    while True:
        try:
            # TODO: PROMPT FOR GRANULARITY

            # load selection table(s)
            df = from_raven(path, tax, timestamp_parser=timestamp_parser)

            # add missing fields
            # (set file ID to zero (0) if file is missing)
            df["job_id"] = job_id
            df["deployment_id"] = deployment_id
            df["file_id"] = df.path.apply(lambda x: file_id_map.get(os.path.basename(x), 0))

            # drop columns
            df = df.drop(columns=["path"])

            # if the selection table contains any tags, check if they need to be added to the database
            if "tag" in df.columns.values:
                tags = pd.unique(df.tag)
                tags = tags[~tags.isna()]
                tags = tags.tolist()
                new_tags = []
                for tag in tags:
                    tag_id = c.execute(f"SELECT id FROM tag WHERE name = '{tag}'").fetchone()
                    if tag_id is None:
                        new_tags.append(tag)

                if len(new_tags) > 0:
                    cprint(f"\n ## The selection table contains the following new tags: {new_tags}", "red")

                    proceed = ui.UserInputYesNo(
                        "add_tags", 
                        "Add the new tags to the database? [y/N]  (required to proceed with submission)", 
                    ).request()

                    if not proceed:
                        terminate(conn)

                    add_tags(conn, tags)
                    
            # insert into db
            cprint(f"\n ## Adding {len(df)} annotations to the database ...", "yellow")

            start = datetime.now()
            annot_ids = kdb.add_annotations(conn, annot_tbl=df, job_id=job_id, progress_bar=True)
            end = datetime.now()

            # print summary
            print_annotation_summary(conn, annot_ids)

            break

        except KeyboardInterrupt:
            terminate(conn)

        except Exception:
            cprint("\n ## "+ traceback.format_exc(), "red")
            cprint(" ## Error processing selection table", "red")
            terminate(conn)


    # commit changes
    #conn.commit()

    cprint(f"\n ## Successfully added {len(annot_ids)} annotations to the database in {(end - start).total_seconds():.2f} seconds", "yellow")

    # check if any of the annotations just added pertain to audio files not present in the database
    c = conn.cursor()
    query = f"SELECT file_id FROM annotation WHERE id in {list_to_str(annot_ids)}"
    rows = c.execute(query).fetchall()
    num_missing = 0
    for row in rows:
        num_missing += (row[0] == 0)

    if num_missing > 0:
        cprint(f"\n ## WARNING: {num_missing} of the annotations pertain to audio files not present in the database", "red")


def add_tags(conn, tags):
    """ Interactive session for adding new annotations to the database.

        Args:
            conn: sqlite3.Connection
                Database connection
            tags: str, list(str)
                Tag name(s)

        Returns:
            None
    """
    if not isinstance(tags, list):
        tags = [tags]

    for tag_name in tags:
        cprint(f"\n ## Adding the tag `{tag_name}` to the database ...", "yellow")

        x = ui.UserInput(
            group="annotation",
            name="tag_description",
            message="Tag description",
        )
        x.add_option(
            key=["v","view"],
            message="View existing tags",
            fcn=lambda x: vw.view_tags(conn)
        )

        tag_description = x.request()

        v = {
            "name": tag_name, 
            "description": tag_description,
        }
        kdb.insert_row(conn, table_name="tag", values=v)

        # commit changes
        #conn.commit()

        cprint(f"\n ## Successfully added the tag `{tag_name}` to the database", "yellow")


def from_raven(input_path, tax, sep=None, granularity="unit", timestamp_parser=None):
    """ Loads entries from a RavenPro selections table

        Args:
            input_path: str
                Path to the RavenPro selection table(s). Use of wildcards is allowed.
            tax: korus.tax.AcousticTaxonomy
                Annotation taxonomy
            sep: str
                Character used to separate columns in the selection table. If None, tries to auto-detect the separator character.
            granularity: str
                Default granularity of annotations. 
            timestamp_parser: callable
                Given a filename (str) returns the UTC timestamp embedded 
                in the filename as a datetime object. Optional.

        Returns:
            df_out: pandas.DataFrame
                Table of selections

        Raises:
            FileNotFoundError: if the input file does not exist.
            ValueError: if the input table contains fields with invalid data types
    """
    # find files
    input_paths = glob(input_path)

    cprint(f"\n ## Found {len(input_paths)} selection tables matching the search criteria", "yellow")

    # load files
    dfs = []
    for input_path in input_paths:
        # detect separator
        if sep is None:
            with open(input_path, "r") as f:
                header = f.readline()
                if "\t" in header: sep = "\t"
                elif ";" in header: sep = ";"
                elif "," in header: sep = ","

        # load selections
        dfs.append(pd.read_csv(input_path, sep=sep))

    # concatenate
    df_in = pd.concat(dfs, ignore_index=True)

    # required columns
    required_cols = [
        "Begin File",
    ]

    # optional columns w/ default values
    optional_cols = {
        "Channel": 1,
        "File Offset (s)": 0,
        "Delta Time (s)": None,
        "Low Freq (Hz)": 0,
        "High Freq (Hz)": None,
        "Sound Source": None,
        "Sound Type": None,
        "Tentative Sound Source": None,
        "Tentative Sound Type": None,
        "Ambiguous Sound Source": None,
        "Ambiguous Sound Type": None,
        "Comments": None,
        "Valid": 1,
    }

    has_duration = ("Delta Time (s)" in df_in.columns)
    has_freq_max = ("High Freq (Hz)" in df_in.columns)
    has_ambiguous = ("Ambiguous Sound Source" in df_in.columns)

    # check that required columns are present
    for c in required_cols:
        fail_msg = f"required column {c} missing in input file: {input_path}"
        assert c in df_in.columns, fail_msg

    # add default values for missing, optional columns
    for c,v in optional_cols.items():
        if c not in df_in.columns:
            df_in[c] = v

    # sort according to filename and start time
    df_in = df_in.sort_values(by=["Begin File", "File Offset (s)"])

    # define structure of output csv file
    data_out = {
        "path": [],
        "start_ms": [],
        "freq_min_hz": [],
        "sound_source": [],
        "sound_type": [],
        "tentative_sound_source": [],
        "tentative_sound_type": [],
        "ambiguous_sound_source": [],
        "ambiguous_sound_type": [],
        "granularity": [],
        "comments": [],
        "channel": [],
        "valid": [],
    }

    df_out = pd.DataFrame(data_out)

    # fill data into output dataframe
    df_out["path"] = df_in["Begin File"]
    df_out["start_ms"] = df_in["File Offset (s)"] * 1000
    df_out["freq_min_hz"] = df_in["Low Freq (Hz)"]
    df_out["sound_source"] = df_in["Sound Source"]
    df_out["sound_type"] = df_in["Sound Type"]
    df_out["tentative_sound_source"] = df_in["Tentative Sound Source"]
    df_out["tentative_sound_type"] = df_in["Tentative Sound Type"]
    df_out["ambiguous_sound_source"] = df_in["Ambiguous Sound Source"]
    df_out["ambiguous_sound_type"] = df_in["Ambiguous Sound Type"]
    df_out["comments"] = df_in["Comments"]
    df_out["channel"] = df_in["Channel"] - 1
    df_out["valid"] = df_in["Valid"]
    df_out["granularity"] = granularity

    if has_duration:
        df_out["duration_ms"] = df_in["Delta Time (s)"] * 1000

    if has_freq_max:
        df_out["freq_max_hz"] = df_in["High Freq (Hz)"]

    # if the Selection Table contains the columns 'Granularity' or 'Batch Annotation', use them (in that order of preference)
    if "Granularity" in df_in.columns.values:
        df_out["granularity"] = df_in["Granularity"].apply(lambda x: x.lower() if isinstance(x, str) else granularity)

    elif "Batch Annotation" in df_in.columns.values:
        idx_batch = (df_in["Batch Annotation"] == 1) 
        df_out.loc[idx_batch, "granularity"] = "batch"

    # if the Selection Table contains the column 'Tag', use it
    if "Tag" in df_in.columns.values:
        df_out["tag"] = df_in["Tag"]

    # replace NaN values
    df_out.comments = df_out.comments.fillna("")
    df_out.sound_source = df_out.sound_source.fillna("")
    df_out.sound_type = df_out.sound_type.fillna("")
    df_out.tentative_sound_source = df_out.tentative_sound_source.fillna("")
    df_out.tentative_sound_type = df_out.tentative_sound_type.fillna("")
    df_out.ambiguous_sound_source = df_out.ambiguous_sound_source.fillna("")
    df_out.ambiguous_sound_type = df_out.ambiguous_sound_type.fillna("")

    # validate data
    # also detect and parse ambiguous assignments in confident/tentative columns
    if not has_ambiguous:
        df_out = _validate_annotations(df_out, tax)

    # parse timestamps from filenames
    if timestamp_parser is not None:
        def get_start_utc(row):
            """ Helper function for obtaining UTC start times """
            return timestamp_parser(row.path) + timedelta(microseconds=row.duration_ms * 1E3)

        df_out["start_utc"] = df_out.apply(lambda r: get_start_utc(r), axis=1)

    # dictionary to specify column types
    dtype_dict = {
        "path": "string",
        "start_ms": np.float32,
        "freq_min_hz": np.float32,
        "sound_source": "string",
        "sound_type": "string",
        "tentative_sound_source": "string",
        "tentative_sound_type": "string",
        "ambiguous_sound_source": "string",
        "ambiguous_sound_type": "string",
        "granularity": "string",
        "comments": "string",
        "channel": np.uint8,
        "tag": "string",
        "valid": np.uint8,
    }

    if has_duration:
        dtype_dict["duration_ms"] = np.float32

    if has_freq_max:
        dtype_dict["freq_max_hz"] = np.float32

    # cast columns, one at the time, for improved error handling
    for col_name, col_dtype in dtype_dict.items():
        try:
            if col_name in df_out.columns.values:
                df_out = df_out.astype({col_name: col_dtype})

        except:
            # if columns could not be casted to the desired type,
            # list the unique values in the column and their counts,
            # and issue a ValueError
            counts = df_out[col_name].value_counts(dropna=False)
            msg = (
                f"unable to cast {col_name} to {col_dtype} becase the "
                + "column contains entries with an invalid type; "
                + "column values are (value:count):"
            )
            for value, count in counts.items():
                msg += f" {value}:{count},"
            raise ValueError(msg)

    # replace empty sound source with root tag
    df_out.sound_source = df_out.sound_source.apply(lambda x: x if x != "" else tax.get_node(tax.root).tag)

    # replace empty tentative/ambiguous sound source with None
    df_out.tentative_sound_source = df_out.tentative_sound_source.apply(lambda x: x if x != "" else None)
    df_out.ambiguous_sound_source = df_out.ambiguous_sound_source.apply(lambda x: x if x != "" else None)

    # replace empty sound type with root tag
    def fcn(r):
        if r.sound_type != "": 
            return r.sound_type
        else:
            try:
                types = tax.sound_types(r.sound_source)   
                return types.get_node(types.root).tag
            except AttributeError:
                return "Unknown"

    df_out.sound_type = df_out.apply(lambda r: fcn(r), axis=1)

    # replace empty tentative sound type with None
    df_out.tentative_sound_type = df_out.tentative_sound_type.apply(lambda x: x if x != "" else None)
    df_out.ambiguous_sound_type = df_out.ambiguous_sound_type.apply(lambda x: x if x != "" else None)

    # round to appropriate number of digits
    decimals = {"start_ms": 0, "freq_min_hz": 1}
    if has_duration: 
        decimals["duration_ms"] = 0
    if has_freq_max:
        decimals["freq_max_hz"] = 1

    df_out = df_out.round(decimals)

    return df_out


def print_annotation_summary(conn, indices):
    """ Print annotation summary
    
        Args:
            conn: sqlite3.Connection
                Database connection
            indices: list(int)
                Indices in the annotation table    
    """
    df = kdb.get_annotations(conn, indices)

    # validity
    counts = df.value_counts("valid", dropna=False).sort_index()
    counts = pd.DataFrame({"Count": counts})
    print(tabulate(counts, headers=["Valid", "Count"], tablefmt='psql'))

    # granularity
    counts = df.value_counts("granularity", dropna=False).sort_index()
    counts = pd.DataFrame({"Count": counts})
    print(tabulate(counts, headers=["Granularity", "Count"], tablefmt='psql'))

    # tags
    counts = df.value_counts("tag", dropna=True)
    counts = pd.DataFrame({"Count": counts})
    print(tabulate(counts, headers=["Tag", "Count"], tablefmt='psql'))

    # confident labels
    counts = df.value_counts(["sound_source", "sound_type"], dropna=True)
    counts = pd.DataFrame({"Count": counts})
    print(tabulate(counts, headers=['Label', "Count"], tablefmt='psql'))

    # tentative labels
    counts = df.value_counts(["tentative_sound_source", "tentative_sound_type"], dropna=True)
    counts = pd.DataFrame({"Count": counts})
    print(tabulate(counts, headers=["Tentative label", "Count"], tablefmt='psql'))

    # ambiguous labels
    counts = df.value_counts("ambiguous_label", dropna=True)
    counts = pd.DataFrame({"Count": counts})
    print(tabulate(counts, headers=["Ambiguous label", "Count"], tablefmt='psql'))


def _validate_annotations(df, tax):
    """ Helper function for validating annotation data.
    
        Also parses ambiguous label assignments separated by slash (/).
     
        The function checks the confident/tentative label columns for occurrences of slash (/) characters.

        When ambiguous labels are found, they are moved to the corresponding ambiguous column. 

        If they occur in the confident column, they are replaced by the last common ancestor node in the taxonomy. 
        If they occur in the tentative column, they are replaced by a 'null' value.

        Args:
            df: pandas DataFrame
                Selection table
            tax: korus.tax.AcousticTaxonomy
                Annotation taxonomy

        Returns:
            df: pandas DataFrame
                Selection table with ambiguous assignments resolved.
    """
    # maps for invalid labels
    src_map = dict()
    typ_map = dict()

    def _parse_ambiguous(labels, tax, label_map, idx, row, sep="/"):
        """ Helper function for parsing and validating ambiguous label assignments """
        labels_val = []
        for label in labels.split(sep):
            label, label_map = _validate_label(label, tax, label_map, idx, row)
            labels_val.append(label)

        return labels_val, label_map

    # loop over all entries
    for idx,row in df.iterrows():     

        # repeat until all validations pass
        while True:

            try:
                # 1) parse ambiguous assignments separated by slash (/)

                if row.ambiguous_sound_source == "":
                    # sound source
                    s = row["sound_source"]
                    if "/" in s:
                        sources, src_map = _parse_ambiguous(s, tax, src_map, idx, row, sep="/")
                        parent = tax.last_common_ancestor(sources)
                        row["ambiguous_sound_source"] = ",".join(sources)
                        row["sound_source"] = parent

                    # tentative sound source
                    s = row["tentative_sound_source"]
                    if "/" in s:
                        sources, src_map = _parse_ambiguous(s, tax, src_map, idx, row, sep="/")
                        row["ambiguous_sound_source"] = ",".join(sources)
                        row["tentative_sound_source"] = ""

                if row.ambiguous_sound_type == "":
                    s = row["sound_source"]
                    typ_tax = tax.sound_types(s)

                    # sound type
                    t = row["sound_type"]
                    if "/" in t:            
                        types, typ_map = _parse_ambiguous(t, typ_tax, typ_map, idx, row, sep="/")                
                        parent = typ_tax.last_common_ancestor(types)
                        row["ambiguous_sound_type"] = ",".join(types)
                        row["sound_type"] = parent

                    # tentative sound type
                    t = row["tentative_sound_type"]
                    if "/" in t:
                        types, typ_map = _parse_ambiguous(t, typ_tax, typ_map, idx, row, sep="/")                
                        row["ambiguous_sound_type"] = ",".join(types)
                        row["tentative_sound_type"] = ""


                # 2) validate labels

                ss = row["sound_source"]
                row["sound_source"], src_map = _validate_label(ss, tax, src_map, idx, row)

                tss = row["tentative_sound_source"]
                row["tentative_sound_source"], src_map = _validate_label(tss, tax, src_map, idx, row)

                ass = row["ambiguous_sound_source"]
                sources, src_map = _parse_ambiguous(ass, tax, src_map, idx, row, sep=",")
                row["ambiguous_sound_source"] = ",".join(sources)

                st = row["sound_type"]
                typ_tax = tax.sound_types(s)
                row["sound_type"], typ_map = _validate_label(st, typ_tax, typ_map, idx, row)

                tst = df.loc[idx, "tentative_sound_type"]
                if tss is not None and tss != "":
                    typ_tax = tax.sound_types(tss)
                
                row["tentative_sound_type"], typ_map = _validate_label(tst, typ_tax, typ_map, idx, row)

                ast = row["ambiguous_sound_type"]
                types, typ_map = _parse_ambiguous(ast, typ_tax, typ_map, idx, row, sep=",")
                row["ambiguous_sound_type"] = ",".join(types)

                # 3) all validations have passed so exit the loop

                df.loc[idx] = row                       
                break        


            except ValueError:
                # edit row manually
                row = edit_row_manually(idx, row)

    # replace NaN values
    df.comments = df.comments.fillna("")
    df.sound_source = df.sound_source.fillna("")
    df.sound_type = df.sound_type.fillna("")
    df.tentative_sound_source = df.tentative_sound_source.fillna("")
    df.tentative_sound_type = df.tentative_sound_type.fillna("")
    df.ambiguous_sound_source = df.ambiguous_sound_source.fillna("")
    df.ambiguous_sound_type = df.ambiguous_sound_type.fillna("")

    return df


def edit_row_manually(idx, row):
    """ Edit an annotation manually.

        The annotation data is saved to a temporary YAML file. The user is prompted via the console 
        to edit and save the file, then hit ENTER to proceed.
        
        Datetime objects are converted to strings using the format `%Y-%m-%d %H:%M:%S.%f`

        Args:
            idx: int
                Index
            row: pandas Series
                Values

        Returns:
            row: pandas Series
                The edited values
    """
    path = f"korus-entry-{idx}.yaml"

    with open(path, "w") as f:
        row_dict = row.to_dict()
        for k,v in row_dict.items():
            if isinstance(v, datetime):
                row_dict[k] = v.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

        yaml.dump(row_dict, f)      

    try:
        msg = f" >> Entry {idx} saved to {path}. Edit file manually and save. Then hit ENTER to proceed with submission."
        input(msg)
    
    except KeyboardInterrupt:
        os.remove(path)
        raise

    with open(path, "r") as f:
        row_dict = yaml.safe_load(f)

    row = pd.Series(row_dict)

    os.remove(path)

    return row


def _validate_label(x, tax, label_map, idx, row):
    """ Helper function for validating labels.

        If the label is found to be invalid, the user is prompted via the terminal for an alternative label.

        Args:
            x: str
                Label to be validated
            tax: Korus.tax.AcousticTaxonomy
                Taxonomy of allowed labels, against which x will be validated
            label_map: dict
                Mapping for invalid labels
            idx: int
                Row index. Only used for interactive prompt.
            row: pandas Series
                Row values. Only used for interactive prompt.

        Returns:
            y: str
                The validated label.
            label_map: dict
                The updated label map.

        Raises:
            ValueError: if the user requests to switch to manual editing mode
    """
    if x is None or x == "":
        return x, label_map

    y = x
    while tax.get_id(y) is None:
        if y in label_map:
            y = label_map[y]

        else:
            try:
                cprint(f" >> Unrecognized label in entry {idx}: {y}", "red")
                msg = " >> Options:"
                msg += "\n >>  - [name]:      alternative, valid label"
                msg += "\n >>  - v/view:      view entry"
                msg += "\n >>  - t/taxonomy:  view taxonomy"
                msg += "\n >>  - m/manual:    switch to manual editing mode"
                msg += "\n >>  - Ctrl-C:      abort\n"

                res = input(msg)

                if res in ["v","view"]:
                    tbl = [[k,v] for k,v in row.to_dict().items()]
                    print(tabulate(tbl, headers=["Field","Value"], tablefmt="psql"))                    

                if res in ["t","taxonomy"]:
                    tax.show()

                if res in ["m","manual"]:
                    raise ValueError

                else:
                    y = res

            except KeyboardInterrupt:
                raise

    if y != x:
        cprint(f" >> Replacing label in entry {idx}: {x} -> {y}", "green")
        
        if x not in label_map:
            replace = ui.UserInputYesNo(
                "label_replacement_rule", 
                "Apply same replacement rule to all other entries? [y/N]", 
            ).request()
            if replace:
                label_map[x] = y

    return y, label_map


