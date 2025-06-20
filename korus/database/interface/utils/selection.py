import os
import warnings
import numpy as np
import pandas as pd
from datetime import datetime, timedelta


def map_to_audiofile(
    row: pd.Series,
    window: float,
    view_centers: list[datetime],
    files: pd.DataFrame,
    data_support: bool,
) -> pd.DataFrame:
    """Helper function for `create_selections` method in AnnotationInterface class.

    Maps the annotation 'views' to their audiofile positions.

    Args:
        row: Pandas Series
            A single row in the annotation table
        window: float
            Selection window size in seconds
        view_centers: list[datetime.datetime]
            UTC timestamps of the views
        files: pandas DataFrame
            File table. Must have columns `deployment_id`, `start_utc`, `end_utc`, `absolute_path`, and `duration`
            and entries should be sorted by deployment first, and chronologically second.
        data_support: bool
            If True, selection windows are not allow to extend beyond the start/end times of the audio files in the database.

    Returns:
        : pandas DataFrame
            Selection; has columns: sel_id, filename, start, end

    """
    # convert to timedelta objects
    window = timedelta(seconds=window)

    # selection view start/end times in seconds relative to file start
    delta = np.array(
        [((x - 0.5 * window) - row.start_utc).total_seconds() for x in view_centers]
    )
    start = row.start + delta
    end = start + window.total_seconds()

    # file path
    file_path = files.loc[row.file_id].absolute_path

    # file UTC start time
    file_start_utc = files.loc[row.file_id].start_utc

    # file duration
    file_duration = files.loc[row.file_id].duration

    # first, collect data for views that are fully within the audio file
    idx = (start >= 0) & (end <= file_duration)
    df = pd.DataFrame(
        {
            "sel_id": np.arange(len(start[idx])),
            "filename": file_path,
            "start": start[idx],
            "end": end[idx],
        }
    )

    # if all views are within the file, end here
    if np.sum(idx) == len(start):
        return df

    # otherwise, proceed to deal with views that extend beyond the file limits
    start = start[~idx]
    end = end[~idx]
    data = []
    sel_id = df.shape[0]
    for view_start, view_end in zip(start, end):

        view_start_utc = file_start_utc + timedelta(seconds=view_start)
        view_end_utc = file_start_utc + timedelta(seconds=view_end)

        # find all 'supporting' files that overlap with the view
        idx = (
            (files.deployment_id == row.deployment_id)
            & (files.start_utc <= view_end_utc)
            & (files.end_utc >= view_start_utc)
        )

        if np.sum(idx) == 0:
            msg = (
                f"Could not find supporting audio file for selection view starting at {view_start_utc}"
                + f" and ending at {view_end_utc} for deployment {row.deployment_id}"
            )
            warnings.warn(msg, UserWarning)
            continue

        segments = []
        tot_duration = 0
        tot_gap = 0
        prev_file_end_utc = None

        # loop over supporting files
        for _, f in files.loc[idx].iterrows():
            segment_start_utc = max(view_start_utc, f.start_utc)
            segment_start = (segment_start_utc - f.start_utc).total_seconds()

            segment_end_utc = min(view_end_utc, f.end_utc)
            segment_end = (segment_end_utc - f.start_utc).total_seconds()

            segments += [[sel_id, f.absolute_path, segment_start, segment_end]]

            if prev_file_end_utc is not None:
                tot_gap += (f.start_utc - prev_file_end_utc).total_seconds()

            tot_duration += segment_end - segment_start
            prev_file_end_utc = f.end_utc

        # expand at beginning to account for inter-file gaps
        segments[0][2] -= tot_gap
        tot_duration += tot_gap
        tot_duration = np.round(tot_duration, 4)

        if data_support and (
            tot_duration < window.total_seconds() or segments[0][2] < 0
        ):
            # discard the view, if there is insufficient data support
            continue

        elif not data_support:
            raise NotImplementedError("data_support=False not yet implemented")

        data += segments
        sel_id += 1

    if len(data) > 0:
        df_beyond = pd.DataFrame(data, columns=["sel_id", "filename", "start", "end"])
        df = pd.concat([df, df_beyond])

    return df


def compute_view_centers(
    row: pd.Series, window: float, step: float, center: bool
) -> list[datetime]:
    """Helper function for `create_selections` method in AnnotationInterface class.

    Computes the temporal midpoints of the view(s) created of each annotation.

    Args:
        row: Pandas Series
            A single row in the annotation table. Must have attributes `start_utc`, `duration`, `num_view`.
        window: float
            Window size in seconds
        step: float
            Step size in seconds
        center: bool
            Whether the view(s) should be centered on the annotation.

    Args:
        view_centers: list[datetime.datetime]
            Temporal midpoint of each view
    """
    # convert to timedelta objects
    window = timedelta(seconds=window)
    step = timedelta(seconds=step) if step is not None else None

    # grab annotation data
    annot_start = row.start_utc
    annot_delta = timedelta(seconds=row.duration)
    annot_end = annot_start + annot_delta
    annot_center = annot_start + 0.5 * annot_delta

    # wide/narrow annotation
    is_wide = annot_delta > window

    # primary selection
    if center:
        view_center = annot_center
    else:
        view_center = annot_center + (np.random.rand() - 0.5) * (annot_delta - window)

    # time-translated views
    if row.num_view == 1:
        view_centers = [view_center]

    else:
        # step backward in time
        if is_wide:
            nb = (
                view_center - 0.5 * window - annot_start
            ).total_seconds() / step.total_seconds()
        else:
            nb = (
                view_center + 0.5 * window - annot_end
            ).total_seconds() / step.total_seconds()

        nb = np.floor(nb).astype(int)

        # step forward in time
        if is_wide:
            nf = (
                annot_end - view_center - 0.5 * window
            ).total_seconds() / step.total_seconds()
        else:
            nf = (
                annot_start - view_center + 0.5 * window
            ).total_seconds() / step.total_seconds()

        nf = np.floor(nf).astype(int)

        no_steps = np.arange(start=-nb, stop=nf + 1, step=1)

        # if the number of selection windows is capped and we have too many selections, randomly subsample as many as we need
        if row.num_view > 1 and row.num_view < len(no_steps):
            no_steps = np.random.choice(no_steps, size=row.num_view, replace=False)

        # midpoint(s) of the selection windows (datetime objects)
        view_centers = [view_center + i * step for i in no_steps]

    return view_centers


def compute_number_of_views(
    df: pd.DataFrame, window: float, num_max: int, stepping: bool
) -> pd.DataFrame:
    """Helper function for `create_selections` method in AnnotationInterface class.

    Computes the number of selections to be created from each annotation, also referred to as the number of `views`.

    The result is stored in a column named `num_view` which is added to the input data frame.

    Note: If `num_max` is None (i.e. no limit) `num_view` is set to -1.

    Args:
        df: Pandas DataFrame
            Annotation table. Must have column `duration`.
        window: float
            Window size in seconds.
        num_max: int
            Create at most this many selections.
        stepping: bool
            Whether multiple, time-translated views of each annotation are to be created.

    Args:
        df: Pandas DataFrame
            Annotation table with added column 'num_view'

    """
    df["num_view"] = -1 if stepping else 1

    if num_max is None:
        return df

    # if the total number of selections is capped, and stepping is enabled,
    # make the number of views of each annotation approx. proportional to max(window, duration)
    if stepping:
        df.num_view = df.duration.apply(lambda x: max(x, window))
        df.num_view *= num_max / df.num_view.sum()

        def round_fcn(val, rndm):
            val_floor = np.floor(val)
            return val_floor + ((val - val_floor) > rndm)

        df["rand"] = np.random.rand(df.shape[0])
        df.num_view = df.apply(lambda r: round_fcn(r.num_view, r.rand), axis=1).astype(
            "int"
        )
        df.drop(columns="rand", inplace=True)

    # if stepping is disabled, each annotation contributes 0 or 1 selection, randomly sampled
    elif num_max < df.shape[0]:
        df.num_view = 0
        idx = np.random.choice(np.arange(df.shape[0]), size=num_max, replace=False)
        df.loc[idx, "num_view"] = 1

    # if we have too many views, randomly drop some to stay below limit
    n_excess = np.sum(df.num_view) - num_max
    if n_excess > 0:
        # create array of indices, where every index is represented num_view times
        indices = np.concatenate(
            [
                np.ones(row.num_view) * idx
                for idx, row in df.loc[df.num_view > 0].iterrows()
            ]
        )
        # randomly select n_excess elements from the array
        for idx in np.random.choice(indices, size=n_excess, replace=False):
            # reduce num_view by 1
            df.loc[idx, "num_view"] -= 1

    return df
