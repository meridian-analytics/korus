import inquirer
import pandas as pd
from korus.database.database import Database
from korus.database.interface import TableViewer
import korus.cli.text as txt
import korus.cli.prompt.prompt as prompt
from korus.cli.cursor import cursor


def label_as_str(label: tuple | list[tuple]) -> str:
    """Helper function for encoding (source,type) labels as strings"""
    if isinstance(label, tuple):
        label = [label]

    if label is None:
        return ""

    else:
        label = [f"{x[0]},{x[1]}" for x in label]
        return "; ".join(label)


def view_taxonomy(db: Database):
    """View the taxonomy tree

    Args:
        db: korus.database.Database
            The database instance
    """
    # prompt user to specify taxonomy version
    msg = "Enter the " + txt.bold("id") + " of the taxonomy you wish to view"
    taxonomy_id = prompt.enter_index(db, "taxonomy", msg)

    tax = db.taxonomy.get_taxonomy(taxonomy_id)

    # print the entire sound-source tree
    tax.show()

    # let user inspect individual sound sources and types
    while True:

        # select a sound source
        msg = (
            str(cursor)
            + "Enter a sound-source label, or hit Ctrl-C to return to the previous menu"
        )

        def validate(answers, current):
            if tax.label_exists(current):
                return True
            else:
                reason = f"The taxonomy does not contain the sound source `{current}`"
                raise inquirer.errors.ValidationError("", reason=reason)

        try:
            sound_source_label = inquirer.text(msg, validate=validate)
        except KeyboardInterrupt:
            break

        sound_source = tax.get_node(sound_source_label)

        # print node data
        print(f"\nlabel: {sound_source.tag}")
        for k, v in sound_source.data.items():
            if k != "sound_types":
                print(f"k: {v}")

        print("sound types:")
        sound_types = tax.sound_types(sound_source_label)
        sound_types.show()

        # select a sound type
        msg = (
            str(cursor)
            + "Enter a sound-type label, or hit Ctrl-C to return to the previous menu"
        )

        def validate(answers, current):
            if tax.label_exists(sound_source_label, current):
                return True
            else:
                reason = f"The taxonomy does not contain the sound type `{current}` for the sound source `{sound_source_label}`"
                raise inquirer.errors.ValidationError("", reason=reason)

        try:
            sound_type_label = inquirer.text(msg, validate=validate)
        except KeyboardInterrupt:
            continue

        sound_type = sound_types.get_node(sound_type_label)

        # print node data
        print(f"\nlabel: {sound_type.tag}")
        for k, v in sound_type.data.items():
            print(f"k: {v}")


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


def view_contents(db: Database, table_name: str):
    """View table contents

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

    # for each table, specify default field choices and transforms
    if table_name == "taxonomy":
        defaults = ["version", "timestamp", "changes", "comment"]
        transforms = {
            "timestamp": lambda x: x.strftime("%Y-%m-%d %H:%M:%S"),
            "changes": lambda x: None if x is None else " | ".join(x),
        }

    elif table_name == "deployment":
        defaults = [
            "name",
            "start_utc",
            "end_utc",
            "latitude_deg",
            "longitude_deg",
            "depth_m",
        ]
        transforms = {
            "start_utc": lambda x: (
                "" if x is None or pd.isna(x) else x.strftime("%Y-%m-%d %H:%M:%S")[:-3]
            ),
            "end_utc": lambda x: (
                "" if x is None or pd.isna(x) else x.strftime("%Y-%m-%d %H:%M:%S")[:-3]
            ),
        }

    elif table_name == "file":
        defaults = tbl.field_names
        transforms = {
            "start_utc": lambda x: (
                ""
                if x is None or pd.isna(x)
                else x.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            ),
            "end_utc": lambda x: (
                ""
                if x is None or pd.isna(x)
                else x.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            ),
        }

    elif table_name == "job":
        defaults = [
            "annotator",
            "completion_date",
            "is_exhaustive",
            "target",
            "taxonomy_id",
            "comments",
        ]
        transforms = {
            "completion_date": lambda x: (
                "" if x is None or pd.isna(x) else x.strftime("%Y-%m-%d")
            ),
        }

    elif table_name == "annotation":
        defaults = [
            "deployment_id",
            "start_utc",
            "duration",
            "label",
            "tentative_label",
            "ambiguous_label",
            "multiple_label",
            "excluded_label",
            "tag",
            "comments",
        ]
        transforms = {
            "start_utc": lambda x: (
                ""
                if x is None or pd.isna(x)
                else x.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            ),
            "label": label_as_str,
            "tentative_label": label_as_str,
            "ambiguous_label": label_as_str,
            "multiple_label": label_as_str,
            "excluded_label": label_as_str,
        }

    else:
        # for all other tables, include all fields
        defaults = tbl.field_names
        transforms = None

    # prompt user to select which fields to display
    msg = str(cursor) + "Select the fields you wish to display"
    field_names = inquirer.checkbox(
        msg,
        choices=tbl.field_names + tbl.alias_names,
        default=defaults,
    )

    viewer = TableViewer(tbl, field_names, transforms=transforms)
    counter = 0
    for page in iter(viewer):
        if counter >= 1:
            proceed = inquirer.confirm("Continue?", default=True)
            if not proceed:
                break

        print(page)
        counter += 1
