from datetime import datetime, timedelta
import pandas as pd


def find_gaps(files: pd.DataFrame, annots: pd.DataFrame, max_file_gap: float = 0.1):
    """Find time periods without annotations.

    OBS: Expects all files to have known UTC start times.

    Args:
        files: pandas.DataFrame
            Table of audio files.
        annots: pandas.DataFrame
            Table of annotations.
        max_file_gap: float
            Maximum temporal gap between audiofiles in seconds.
            Inter-annotation gaps are allowed to span multiple audio files provided the temporal gap between the files is below this value.

    Returns:
    """
    # make copies so we don't modify the input args
    files = files.copy()
    annots = annots.copy()

    # set index for file table
    files = files.reset_index().set_index("filename")

    # add start and end times to annotation table
    annots["start_utc"] = annots.apply(
        lambda r: files.loc[r.filename].start_utc
        + timedelta(microseconds=r.start_ms * 1e3),
        axis=1,
    )
    annots["end_utc"] = annots.apply(
        lambda r: r.start_utc + timedelta(microseconds=r.duration_ms * 1e3), axis=1
    )
