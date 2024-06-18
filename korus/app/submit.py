
import os
import sys
import json
import argparse
from termcolor import colored, cprint
import subprocess
import sqlite3
from datetime import datetime
from korus.util import list_to_str
import korus.db as kdb
import korus.app.app_util.view as vw
import korus.app.app_util.add as add
import korus.app.app_util.ui as ui
import logging
import readline


# tab completion for directory/file paths
readline.parse_and_bind("tab: complete")
readline.set_completer_delims("\t\n=")


cache_dir_path = os.path.join(os.environ['HOME'], ".ktam")
log_path = os.path.join(cache_dir_path, "last_submit.json")
# ensure directory exists
if not os.path.exists(cache_dir_path):
    os.makedirs(cache_dir_path)


def terminate(conn):
    """ Helper function for gracefully terminating the program"""
    print("\n Closing database connection and exiting ...")
    conn.close()
    exit(1)


def main():

    logger = ui.InputLogger()

    # load log of last submission
    if os.path.exists(log_path):
        with open(log_path, "r") as f:
            log = json.loads(f.read())
    else:
        log = dict()

    db_help = "Path to the Korus database (.sqlite)"
    db_default = log.get("db_path")
    if db_default is not None:
        db_help += f" | Default: {db_default}"

    parser = argparse.ArgumentParser("Submit a set of annotations to a Korus database")

    parser.add_argument("--database", default=db_default, type=str, help=db_help)

    parser.add_argument(
        "--log_level",
        default=20,
        type=int,
        help="Logging level (DEBUG:10, INFO:20, WARNING:30, ERROR:40, CRITICAL:50)",
    )

    args = parser.parse_args()

    logging.basicConfig(stream=sys.stderr, level=args.log_level)

    assert args.database is not None, "A database path is required"
    assert os.path.exists(args.database), f"{args.database} does not exist"

    logger.write("db_path", args.database)

    # step 0: open connection to database

    conn = sqlite3.connect(args.database)


    # step 1: specify which hydrophone deployment the audio data is from

    def transform_fcn(x):
        id = int(x)
        c = conn.cursor()
        ids = [row[0] for row in c.execute("SELECT id FROM deployment").fetchall()]
        if id not in ids:
            raise ValueError(f"Database does not contain deployment with ID {id}")
        
        return id

    x = ui.UserInput(
        group="deployment",
        name="id",
        message="Enter deployment ID (1,2,3,...) or add a new deployment",
        transform_fcn=transform_fcn,
    )
    x.add_option(
        key=["v","view"],
        message="View existing deployments",
        fcn=lambda x: vw.view_deployments(conn)
    )
    x.add_option(
        key=["n","new"],
        message="New deployment",
        fcn=lambda x: add.add_deployment(conn, logger)
    )

    try:
        deployment_id = x.request(logger)

    except KeyboardInterrupt:
        terminate(conn)


    # step 2: identify/describe the annotation job

    def transform_fcn(x):
        id = int(x)
        c = conn.cursor()
        ids = [row[0] for row in c.execute("SELECT id FROM job").fetchall()]
        if id not in ids:
            raise ValueError(f"Database does not contain job with ID {id}")
        
        return id

    x = ui.UserInput(
        group="job",
        name="id",
        message="Enter job ID (1,2,3,...) or add a new job",
        transform_fcn=transform_fcn,
    )
    x.add_option(
        key=["v","view"],
        message="View existing jobs",
        fcn=lambda x: vw.view_jobs(conn)
    )
    x.add_option(
        key=["n","new"],
        message="New job",
        fcn=lambda x: add.add_job(conn, logger)
    )

    try:
        job_id = x.request(logger)
        new_job = (x.selected_opt and x.selected_opt.message == "New job")

    except KeyboardInterrupt:
        terminate(conn)


    link_more_files = True

    if new_job:
        file_ids = []
    else:
        file_ids = kdb.filter_files(conn, job_id=job_id)

    if len(file_ids) > 0:
        cprint(f" ## There are already {len(file_ids)} audio files linked to this annotation job", "yellow")

        ui_link_more_files = ui.UserInput(
            "link_more_files", 
            "Link more audio files to the annotation job? [y/N]", 
            transform_fcn=lambda x: x.lower() == "y", 
        )

        ui_link_more_files.add_option(
            key=["v","view"],
            message="View linked files",
            fcn=lambda x: vw.view_files(conn, file_ids)
        )
        link_more_files = ui_link_more_files.request()


    if link_more_files:
        # step 3: constrain the time range

        dt_formats = [
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d %H",
            "%Y-%m-%d",
            "%Y-%m",
            "%Y",
        ]
        def to_datetime(x):
            for dt_fmt in dt_formats:
                fmt = dt_fmt if " " in x else dt_fmt[:dt_fmt.find(" ")]
                try:
                    return datetime.strptime(x, fmt)
                except:
                    pass

            raise ValueError("Unrecognized data-time format")

        dt_fmt_human = "YYYY-MM-DD HH:MM:SS.SSS"

        start_utc = ui.UserInput(
            group="audio",
            name="start_utc",
            message=f"UTC start time of the first audio file ({dt_fmt_human})",
            transform_fcn=to_datetime,
            json_fcn=lambda x: x.strftime(dt_fmt),
        ).request(logger)

        end_utc = ui.UserInput(
            group="audio",
            name="end_utc",
            message=f"UTC end time of the last audio file ({dt_fmt_human})",
            transform_fcn=to_datetime,
            json_fcn=lambda x: x.strftime(dt_fmt),
        ).request(logger)


        # step 4: add audio files to the database (optional)

        ui_add_more_files = ui.UserInput(
            "add_more_files", 
            "Add more audio files to the database? [y/N]", 
            transform_fcn=lambda x: x.lower() == "y", 
        )

        while True:
            file_ids = kdb.filter_files(conn, deployment_id, start_utc, end_utc)

            cprint(f" ## The database has {len(file_ids)} audio files from deployment {deployment_id} between {start_utc} and {end_utc}", "yellow")

            if len(file_ids) > 0:
                ui_add_more_files.add_option(
                    key=["v","view"],
                    message="View files",
                    fcn=lambda x: vw.view_files(conn, file_ids)
                )

            if ui_add_more_files.request():
                add.add_files(conn, deployment_id, start_utc, end_utc, logger)
            else:
                break


        # step 5: link audio files to job

        def fcn(x):
            """Helper function for transforming user input for link_files option"""
            # user provides the path to a plain text file with one filename per row
            if os.path.exists(x):
                with open(x, "r") as f:
                    lines = f.readlines()
                    return [os.path.basename(line.strip()) for line in lines]

            # user provides a list of file IDs or filenames
            else:
                return [y if "." in y else int(y) for y in x.split(",")]

        ui_link_files = ui.UserInput(
            "link_files", 
            "Specify which files you want to link to the annotation job, e.g., 1,3,16 or file1.flac,file2.flac or filenames.txt", 
            transform_fcn=fcn,
        )

        ui_link_files.add_option(
            key=["v","view"],
            message="View files",
            fcn=lambda x: vw.view_files(conn, file_ids),
        )

        ui_link_files.add_option(
            key=["a","all"],
            message="Select all files",
            fcn=lambda x: file_ids,
        )

        file_ids = ui_link_files.request()

        # if user specified filenames, look up the table indices
        if isinstance(file_ids[0], str):
            c = conn.cursor()
            filenames_str = list_to_str(file_ids)
            query = f"SELECT id FROM file WHERE filename IN {filenames_str} AND deployment_id = {deployment_id}"
            rows = c.execute(query).fetchall()
            file_ids = [int(row[0]) for row in rows]

            cprint(f" ## Mapped filenames to table indices: {file_ids}", "yellow")

        ui_channel = ui.UserInput(
            "channel", 
            "Which channel(s) were inspected as part of the annotation job? E.g. 0 or 0,1", 
            transform_fcn=lambda x: [int(xi) for xi in x.split(",")],
        )
        ui_channel.add_option(
            key=["d","default"],
            message=f"Use default value: 0",
            fcn=lambda x: 0
        )

        channel = ui_channel.request()

        n_success = kdb.assign_files_to_job(conn, job_id, file_ids, channel=channel)
        conn.commit()

        cprint(f" ## Successfully added {n_success} links to the database", "yellow")


    # step 6: parse the RavenPro selection table

    submit_selection_table = ui.UserInput(
        "submit_selection_table", 
        "Did the annotation job produce any annotations to be added to the database? [y/N]", 
        transform_fcn=lambda x: x.lower() == "y", 
    ).request()

    if submit_selection_table:
        try:
            add.add_annotations(conn, deployment_id, job_id, logger)

        except KeyboardInterrupt:
            terminate(conn)


    # step 7: close connection to database

    conn.close()

    cprint(f" ## Successfully updated local KetoAM database: {args.database}", "yellow")


    # step 8: push to GitLab

    if os.system('git rev-parse') == 0:
        git = True
    else:
        cprint(" ## The current directory is not under Git control", "red")
        git = False

    if git:
        git = ui.UserInput(
            "git", 
            "Commit and push changes to GitLab (recommended)? [y/N]", 
            transform_fcn=lambda x: x.lower() == "y", 
        ).request()

    if not git:
        return

    tag = ui.UserInput(
        "submission_tag", 
        "Provide a tag for the submission",
        transform_fcn=lambda x: x.replace(" ","-"),
    ).request(logger)

    commit_msg = ui.UserInput(
        "commit_message", 
        "Provide a commit message",
    ).request(logger)

    # git:
    # - commit changes and new files to new feature branch
    # - push and create merge request into main
    try:
        subprocess.run(f"git checkout -b {tag}", shell=True)
        subprocess.run(f"git add {args.database}", shell=True)
        subprocess.run(f"git commit -am \"{commit_msg}\"", shell=True)
        subprocess.run(f"git push -o merge_request.create -o merge_request.target=main --set-upstream origin {tag}", shell=True)

        cprint(f" ## Changes successfully committed and pushed to new branch: {tag}", "green")
        cprint(f" ## ACTION REQUIRED: Please review and accept merge request to finalize submission", "yellow")

    except:
        cprint(f" ## ERROR: Failed to commit and push changes to GitLab", "red")


if __name__ == "__main__":
    main()