import os
import numpy as np
import pandas as pd


def read_raven(
    path: str,
    taxonomy,
    file,
    deployment_id: int = None,
    granularity: str = "unit",
    taxonomy_version: int = None,
) -> pd.DataFrame:
    """Read and validate a RavenPro formatted annotation table.

    The validation algorithm verifies that

     * the audiofiles exist in the database
     * the labels exist in the taxonomy

    Args:
        path: str
            Path to the RavenPro file with tab-separated values (TSV).
        taxonomy: korus.database.interface.taxonomy.TaxonomyInterface
            Taxonomy interface
        file: korus.database.interface.annotation.FileInterface
            File interface
        deployment_id: int
            If not specified, the annotation table must contain the column `Deployment ID`.
        granularity: str
            Annotation granularity for entries not marked as 'Batch' annotations.
        taxonomy_version: int
            Acoustic taxonomy that the (source,type) label arguments refer to. If not specified,
            the latest version will be used.

    Returns:
        : pandas.DataFrame
            The validated annotation table, formatted to facilitate ingestion into the Korus database.
            Contains the extra columns,

             * valid (bool): True, if the row was successfully validated. False, if errors were detected.
             * warning (str): Warnings produced by the validation algorithm.
             * error (str): Errors produced by the validation algorithm.
    """
    df_raven = pd.read_csv(path, sep="\t")
    num_entries = len(df_raven)

    if deployment_id is not None:
        df_raven["Deployment ID"] = deployment_id

    assert_msg = "Deployment ID must be specified, either using the `deployment_id` arg or by including a column named `Deployment ID` in the input table."
    assert "Deployment ID" in df_raven.columns, assert_msg

    # other required columns
    required_cols = [
        "Begin File",
    ]

    # optional columns with default values
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
        "Excluded Sound Source": None,
        "Excluded Sound Type": None,
        "Batch": False,
        "Comments": None,
        "Tag": None,
    }

    # check that required columns are present
    for name in required_cols:
        assert_msg = f"Required column {name} missing in input table: {path}"
        assert name in df_raven.columns, assert_msg

    # add default values for missing, optional columns
    for name, value in optional_cols.items():
        if name not in df_raven.columns:
            df_raven[name] = value

    # sort according to filename and start time["FileNotFoundError;"
    df_raven = df_raven.sort_values(by=["Begin File", "File Offset (s)"])

    # map filenames to file IDs
    deployment_ids = df.groupby("Begin File").first()["Deployment ID"]
    filenames = df_raven["Begin File"].unique()
    file_ids = file.get_id(deployment_ids, filenames)
    fname_to_id = {fname: id for fname, id in zip(filenames, file_ids)}

    # define structure of output csv file
    def as_list(x, n=num_entries):
        return [x for _ in range(n)]

    data = {
        "file_id": as_list(None),
        "channel": as_list(0),
        "start": as_list(0),
        "duration": as_list(None),
        "freq_min_hz": as_list(0),
        "freq_max_hz": as_list(None),
        "sound_source": as_list(None),
        "sound_type": as_list(None),
        "tentative_sound_source": as_list(None),
        "tentative_sound_type": as_list(None),
        "excluded_sound_source": as_list(None),
        "excluded_sound_type": as_list(None),
        "ambiguous_sound_source": as_list(None),
        "ambiguous_sound_type": as_list(None),
        "multiple_sound_source": as_list(None),
        "multiple_sound_type": as_list(None),
        "granularity": as_list(None),
        "tag": as_list(None),
        "comments": as_list(None),
        "valid": as_list(True),
        "warning": as_list([]),
        "error": as_list([]),
    }
    df = pd.DataFrame(data)

    # --- enter data ---

    # file ID
    df["file_id"] = df_raven["Begin File"].apply(lambda x: fname_to_id[x])

    # validate file IDs
    idx = df["file_id"] == None
    df.loc[idx, "valid"] = False
    df.loc[idx, "error"] += ["FileNotFoundError"]

    # TODO:
    # parse labels (& = AND, / = OR) and map sound sources and sound types to labels
    # for entries with multiple labels, evaluate all possible combinations
    # check that labels are valid


def export_to_raven(
    path: str,
    annotation,
    file,
    indices: int | list[int] = None,
):
    """Helper function for exporting annotations to a TSV file in RavenPro format.

    Args:
        path: str
            Output path. Overwrites any pre-existing file.
        annotation: korus.database.interface.annotation.AnnotationInterface
            Annotation interface
        file: korus.database.interface.annotation.FileInterface
            File interface
        indices: int | list[int]
            The indices of the annotations to be exported. If None, all annotations are exported.
    """
    # get annotation data
    annots = annotation.get(
        indices,
        fields=[
            "deployment_id",
            "file_id",
            "file_id_list",
            "channel",
            "label",
            "tentative_label",
            "excluded_label",
            "ambiguous_label",
            "multiple_label",
            "start_utc",
            "start",
            "duration",
            "freq_min_hz",
            "freq_max_hz",
            "tag",
            "granularity",
            "comments",
        ],
        as_pandas=True,
        return_indices=True,
    )

    # sort by deployment and chronologically
    annots = annots.sort_values(by=["deployment_id", "start_utc", "duration"])

    # file durations and absolute paths
    file_ids = np.unique(np.concatenate(annots.file_id_list.values))
    file_durations = file.get_duration(file_ids)
    file_paths = file.get_absolute_path(file_ids)
    file_paths_dict = {id: path for id, path in zip(file_ids, file_paths)}

    # file cumulative offsets
    file_offsets = np.cumsum(file_durations) - file_durations[0]
    file_offsets_dict = {x: y for x, y in zip(file_ids, file_offsets)}

    # add offsets and file paths to annot dataframe
    annots["offset"] = annots.file_id.apply(lambda x: file_offsets_dict[x])
    annots["filename"] = annots.file_id.apply(
        lambda x: os.path.basename(file_paths_dict[x])
    )
    annots["path"] = annots.file_id.apply(lambda x: file_paths_dict[x])

    # fill data into output dataframe
    df = pd.DataFrame()
    df["Low Freq (Hz)"] = annots.freq_min_hz
    df["High Freq (Hz)"] = annots.freq_max_hz
    df["Delta Time (s)"] = annots.duration
    df["File Offset (s)"] = annots.start
    df["Begin Time (s)"] = annots["offset"] + df["File Offset (s)"]
    df["End Time (s)"] = annots["offset"] + df["File Offset (s)"] + df["Delta Time (s)"]
    df["Begin Path"] = annots.path
    df["Begin File"] = annots.filename
    df["View"] = "Spectrogram"
    df["Channel"] = annots.channel + 1
    df["Selection"] = np.arange(len(annots)) + 1
    df["Sound Source"] = annots.label.apply(lambda x: "" if x is None else x[0])
    df["Sound Type"] = annots.label.apply(lambda x: "" if x is None else x[1])
    df["Tentative Sound Source"] = annots.tentative_label.apply(
        lambda x: "" if x is None else x[0]
    )
    df["Tentative Sound Type"] = annots.tentative_label.apply(
        lambda x: "" if x is None else x[1]
    )
    df["Granularity"] = annots.granularity
    df["Korus ID"] = annots.index
    df["Comments"] = annots.comments

    # encode lists as & or / -separated strings
    df["Tag"] = annots.tag.apply(lambda x: "" if x is None else " & ".join(x))

    def as_list(x, i):
        return [] if x is None else sorted(list(set([a[i] for a in x])))

    df["Excluded Sound Source"] = annots.excluded_label.apply(
        lambda x: " & ".join(as_list(x, 0))
    )
    df["Excluded Sound Type"] = annots.excluded_label.apply(
        lambda x: " & ".join(as_list(x, 1))
    )

    # overwrite label, if ambiguous or multiple label field is not null
    idx = annots.ambiguous_label.isna()
    df.loc[~idx, "Sound Source"] = annots.ambiguous_label.apply(
        lambda x: " / ".join(as_list(x, 0))
    )
    df.loc[~idx, "Sound Type"] = annots.ambiguous_label.apply(
        lambda x: " / ".join(as_list(x, 1))
    )

    idx = annots.multiple_label.isna()
    df.loc[~idx, "Sound Source"] = annots.multiple_label.apply(
        lambda x: " & ".join(as_list(x, 0))
    )
    df.loc[~idx, "Sound Type"] = annots.multiple_label.apply(
        lambda x: " & ".join(as_list(x, 1))
    )

    # round to appropriate number of digits
    df = df.round(
        {
            "Begin Time (s)": 3,
            "End Time (s)": 3,
            "Delta Time (s)": 3,
            "File Offset (s)": 3,
            "Low Freq (Hz)": 1,
            "High Freq (Hz)": 1,
        }
    )

    # ensure directory exists
    dirpath = os.path.dirname(path)
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)

    # save to file
    df.to_csv(path, index=False, sep="\t")
