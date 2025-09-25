import os
import numpy as np
import pandas as pd
from tqdm import tqdm


# Sound source/type ENUM
SINGLE_VALUE = 0
AMBIGUOUS_VALUE = 1
MULTIPLE_VALUE = 2
INVALID_VALUE = 3


def _parse_source_type(
    row: pd.Series,
    column_name: str,
    errors: list[str],
    default=None,
    split: bool = True,
    allow_ambiguous: bool = True,
    allow_multiple: bool = True,
):
    """Helper function for parsing individual sound-source or sound-type values.

    Returns a str if split is False, and a list of strings otherwise.

    Args:
        row: pd.Series
            A single row in the RavenPro annotation table
        column_name: str
            The column to be parsed, e.g. `Sound Source`
        errors: list[str]
            List for storing error messages from the parsing algorithm
        default: any
            Default value
        split: bool
            If True, split the string at every & and /
        allow_ambiguous: bool
            If False, any occurrence of / will be logged as an error
        allow_multiple: bool
            If True, any occurrence of & will be logged as an error

    Returns:
        x: str | list[str]
            The parsed value(s)
        kind: int
            The source/type ENUM. Only returned if split is True and both ambiguous and mulitple values are allowed.
    """
    kind = SINGLE_VALUE
    x = row[column_name]

    if x is None:
        x = default

    if x is not None:
        x = x.replace(" ", "")

    if x is not None and split:
        ambiguous = "/" in x
        multiple = "&" in x

        if ambiguous:
            kind = AMBIGUOUS_VALUE
            x = x.split("/")

        elif multiple:
            kind = MULTIPLE_VALUE
            x = x.split("&")

        else:
            kind = SINGLE_VALUE
            x = [x]

    if kind == AMBIGUOUS_VALUE and kind == MULTIPLE_VALUE:
        errors.append(f"LabelError in {column_name}: & and / cannot be used together")
        x = default
        kind = INVALID_VALUE

    if kind == AMBIGUOUS_VALUE and not allow_ambiguous:
        errors.append(f"LabelError in {column_name}: Ambiguous label not allowed")
        x = default
        kind = INVALID_VALUE

    if kind == MULTIPLE_VALUE and not allow_multiple:
        errors.append(f"LabelError in {column_name}: Multiple label not allowed")
        x = default
        kind = INVALID_VALUE

    if x is not None and split and allow_ambiguous and allow_multiple:
        return x, kind

    else:
        return x


def _create_labels(
    sources: str | list[str],
    types: str | list[str],
    errors: list[str],
    taxonomy_interface,
    version: int = None,
) -> tuple[str, str] | list[tuple[str, str]]:
    """Helper function for making and validating (source,type) labels.

    Args:
        sources: str | list[str]
            The sound sources
        types: str | list[str]
            The sound types
        errors: list[str]
            List for storing validation errors
        taxonomy_interface: TaxonomyInterface
            The taxonomy table interface
        version: int
            The taxonomy version

    Returns:
        valid_labels: tuple[str,str] | list[tuple[str,str]
            The validated labels
    """
    if sources is None or types is None:
        return None

    is_list = isinstance(sources, list) or isinstance(types, list)

    if not isinstance(sources, list):
        sources = [sources]

    if not isinstance(types, list):
        types = [types]

    # create all possible combinations
    labels = [(s, t) for s in sources for t in types]

    # validate labels
    valid_labels = []
    for label in labels:
        try:
            taxonomy_interface.get_label_id(label, version)
            valid_labels.append(label)

        except ValueError:
            continue

    if len(valid_labels) < max(len(sources), len(types)):
        errors.append("LabelError: Invalid label")

    if not is_list and len(valid_labels) == 1:
        valid_labels = valid_labels[0]

    elif len(valid_labels) == 0:
        valid_labels = None

    return valid_labels


def _parse_labels(row: pd.Series, taxonomy_interface, version: int = None) -> dict:
    """Helper function for parsing the sound-source and sound-type columns in RavenPro annotation tables.

        * Combines sound sources and sound types into Korus labels
        * If multiple values are specified using & or /, evaluate all possible combinations
        * Checks that labels exist in the taxonomy

    Args:
        row: pd.Series
            A single row in the RavenPro annotation table
        taxonomy_interface: TaxonomyInterface
            The taxonomy table interface
        version: int
            The taxonomy version

    Returns:
        res: dict
            Dictionary with keys `label`, `tentative_label`, `excluded_label`, `ambiguous_label`, `multiple_label`, `valid`, `errors`
    """
    tax = (
        interface.current
        if version is None
        else taxonomy_interface.releases[version - 1]
    )

    root = tax.get_node(tax.root).tag

    res = {
        "label": None,
        "tentative_label": None,
        "excluded_label": None,
        "ambiguous_label": None,
        "multiple_label": None,
        "valid": True,
        "errors": [],
    }

    # parse `Sound Source`
    sources, src_kind = _parse_source_type(
        row,
        column_name="Sound Source",
        errors=res["errors"],
        default=root,
    )

    # parse `Sound Type`
    types, typ_kind = _parse_source_type(
        row,
        column_name="Sound Type",
        errors=res["errors"],
        default=root,
    )

    # create labels
    labels = _create_labels(sources, types, res["errors"], taxonomy_interface, version)

    if labels is None:
        res["valid"] = len(res["errors"]) == 0
        return res

    # confident label
    conf_src, conf_typ = tax.last_common_ancestor(labels)

    # ambiguous and multiple labels
    if src_kind in [AMBIGUOUS_VALUE, MULTIPLE_VALUE] or typ_kind in [
        AMBIGUOUS_VALUE,
        MULTIPLE_VALUE,
    ]:
        if (src_kind == AMBIGUOUS_VALUE and typ_kind == MULTIPLE_VALUE) or (
            src_kind == MULTIPLE_VALUE and typ_kind == AMBIGUOUS_VALUE
        ):
            err_msg = "LabelError: & and / cannot be used together"
            res["errors"].append(err_msg)

        elif src_kind == AMBIGUOUS_VALUE or typ_kind == AMBIGUOUS_VALUE:
            res["ambiguous_label"] = labels

        elif src_kind == MULTIPLE_VALUE or typ_kind == MULTIPLE_VALUE:
            res["multiple_label"] = labels

    # parse `Excluded Sound Source`
    sources = _parse_source_type(
        row,
        column_name="Excluded Sound Source",
        errors=res["errors"],
        allow_ambiguous=False,
    )

    # parse `Excluded Sound Type`
    types = _parse_source_type(
        row,
        column_name="Excluded Sound Type",
        errors=res["errors"],
        allow_ambiguous=False,
    )

    # excluded labels
    res["excluded_label"] = _create_labels(
        sources, types, res["errors"], taxonomy_interface, version
    )

    # parse `Tentative Sound Source`
    tent_src = _parse_source_type(
        row,
        column_name="Tentative Sound Source",
        errors=res["errors"],
        split=False,
    )

    #  and `Tentative Sound Type`
    tent_typ = _parse_source_type(
        row,
        column_name="Tentative Sound Type",
        errors=res["errors"],
        split=False,
    )

    if tent_src is None and tent_typ is not None:
        tent_src = conf_src

    if tent_typ is None and tent_src is not None:
        tent_typ = conf_typ

    # tentative label
    res["tentative_label"] = _create_labels(
        tent_src, tent_typ, res["errors"], taxonomy_interface, version
    )

    # confident label
    res["label"] = (conf_src, conf_typ)

    # validation status
    res["valid"] = len(res["errors"]) == 0

    return res


def read_raven(
    path: str,
    taxonomy,
    file,
    deployment_id: int = None,
    granularity: str = "unit",
    taxonomy_version: int = None,
    progress_bar: bool = False,
):
    """Read and validate a RavenPro formatted annotation table.

    The validation algorithm verifies that

     * the audiofiles exist in the database
     * the labels exist in the taxonomy

    In case of multiple labels (AND), use ampersand (&) to separate values.
    In case of ambiguous labels (OR), use slash (/) to separate values.
    Note: It is not possible to use both separators in the same row.

    In case of multiple tags (AND), use ampersand (&) to separate values.

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
        progress_bar: bool
            Whether to display a progress bar.

    Returns:
        df: pandas.DataFrame
            The validated annotation table, formatted to facilitate ingestion into the Korus database.
        df_raven: pandas.DataFrame
            The input table with two extra columns:
             * Valid (bool): True, if the row was successfully validated. False, if errors were detected.
             * Errors (str): Errors produced by the validation algorithm.
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

    # dtypes
    dtypes = {
        "Begin File": str,
        "Channel": int,
        "File Offset (s)": float,
        "Delta Time (s)": float,
        "Low Freq (Hz)": float,
        "High Freq (Hz)": float,
        "Sound Source": str,
        "Sound Type": str,
        "Tentative Sound Source": str,
        "Tentative Sound Type": str,
        "Excluded Sound Source": str,
        "Excluded Sound Type": str,
        "Batch": bool,
        "Comments": str,
        "Tag": str,
    }

    # check that required columns are present
    for name in required_cols:
        assert_msg = f"Required column {name} missing in input table: {path}"
        assert name in df_raven.columns, assert_msg

    # add default values for missing, optional columns
    for name, value in optional_cols.items():
        if name not in df_raven.columns:
            df_raven[name] = value

    # set dtype
    df_raven = df_raven.astype(dtypes)

    # replace 'nan'
    for name, dtype in dtypes.items():
        if dtype == str:
            df_raven = df_raven.replace({name: "nan"}, None)

    # sort according to filename and start time
    df_raven = df_raven.sort_values(by=["Begin File", "File Offset (s)"])

    # map filenames to file IDs
    deployment_ids = df_raven.groupby("Begin File").first()["Deployment ID"]
    filenames = df_raven["Begin File"].unique()
    file_ids = file.get_id(deployment_ids, filenames)
    fname_to_id = {fname: id for fname, id in zip(filenames, file_ids)}

    # helper function for creating lists
    def as_list(x, n=num_entries):
        return [x for _ in range(n)]

    # define structure of output csv file
    data = {
        "file_id": as_list(None),
        "channel": as_list(0),
        "start": as_list(0),
        "duration": as_list(None),
        "freq_min_hz": as_list(0),
        "freq_max_hz": as_list(None),
        "label": as_list(None),
        "tentative_label": as_list(None),
        "excluded_label": as_list(None),
        "ambiguous_label": as_list(None),
        "multiple_label": as_list(None),
        "granularity": as_list(None),
        "tag": as_list(None),
        "comments": as_list(None),
    }
    df = pd.DataFrame(data)

    dtypes = {
        "file_id": int,
        "channel": int,
        "start": float,
        "duration": float,
        "freq_min_hz": float,
        "freq_max_hz": float,
        "label": object,
        "tentative_label": object,
        "excluded_label": object,
        "ambiguous_label": object,
        "multiple_label": object,
        "granularity": str,
        "tag": object,
        "comments": str,
    }

    # add validation columns to input dataframe
    df_raven["Valid"] = as_list(True)
    df_raven["Errors"] = as_list("")

    # --- enter data ---

    # file ID
    df["file_id"] = df_raven["Begin File"].apply(lambda x: fname_to_id[x])

    # validate file IDs
    idx = df["file_id"].isna()
    df_raven.loc[idx, "Valid"] = False
    df_raven.loc[idx, "Errors"] += "FileNotFoundError | "
    
    # for missing file, set ID to -1
    df = df.fillna(value={"file_id": -1})

    # copy data
    df["channel"] = df_raven["Channel"] - 1
    df["start"] = df_raven["File Offset (s)"]
    df["duration"] = df_raven["Delta Time (s)"]
    df["freq_min_hz"] = df_raven["Low Freq (Hz)"]
    df["freq_max_hz"] = df_raven["High Freq (Hz)"]
    df["comments"] = df_raven["Comments"]

    # set dtypes
    df = df.astype(dtypes)

    # tag
    df["tag"] = df_raven["Tag"].apply(
        lambda x: x.split("&") if isinstance(x, str) else None
    )

    # granularity
    df["granularity"] = df_raven["Batch"].apply(lambda x: "batch" if x else granularity)

    # parse labels
    label = []
    tentative_label = []
    excluded_label = []
    ambiguous_label = []
    multiple_label = []
    for idx, row in tqdm(
        df_raven.iterrows(), total=df.shape[0], disable=not progress_bar
    ):
        res = _parse_labels(row, taxonomy, taxonomy_version)

        label.append(res["label"])
        tentative_label.append(res["tentative_label"])
        excluded_label.append(res["excluded_label"])
        ambiguous_label.append(res["ambiguous_label"])
        multiple_label.append(res["multiple_label"])

        df_raven.loc[idx, "Valid"] *= res["valid"]
        df_raven.loc[idx, "Errors"] += " | ".join(res["errors"])

    df["label"] = label
    df["tentative_label"] = tentative_label
    df["excluded_label"] = excluded_label
    df["ambiguous_label"] = ambiguous_label
    df["multiple_label"] = multiple_label

    return df, df_raven


def export_to_raven(
    path: str,
    annotation,
    file,
    indices: int | list[int] = None,
):
    """Helper function for exporting annotations to a TSV file in RavenPro format.

    Values in the `ambiguous_label` field are joined using an slash (/) and replace the value in the `label` field.

    Values in the `multiple_label` field are joined using an ampersand (&) and replace the value in the `label` field.

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

    # overwrite confident label, if ambiguous label is not null
    idx = annots.ambiguous_label.isna()
    df.loc[~idx, "Sound Source"] = annots.ambiguous_label.apply(
        lambda x: " / ".join(as_list(x, 0))
    )
    df.loc[~idx, "Sound Type"] = annots.ambiguous_label.apply(
        lambda x: " / ".join(as_list(x, 1))
    )

    # overwrite confident label, if multiple label is not null
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
