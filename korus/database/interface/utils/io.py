import os
import numpy as np
import pandas as pd


def read_raven(path: str) -> pd.DataFrame:
    """read and validate Raven Pro formatted annotation table"""
    pass


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
    df["Tag"] = annots.tag
    df["Granularity"] = annots.granularity
    df["Korus ID"] = annots.index
    df["Comments"] = annots.comments

    df["Ambiguous Label"] = annots.ambiguous_label
    df["Multiple Label"] = annots.multiple_label
    df["Excluded Label"] = annots.excluded_label

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
