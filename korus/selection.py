import os
import warnings
import numpy as np
import pandas as pd
from tqdm import tqdm
from datetime import datetime, timedelta
import korus.db as kdb
from korus.util import list_to_str


def create_selections(
    conn, 
    indices, 
    window_ms, 
    step_ms=None,
    center=False,
    exclusive=False,
    num_max=None,
    source_type_avoid=None,
    data_support=True,
    progress_bar=False,
    full_path=True,
):
    """ Create uniform-length selection windows on a set of annotations.

        TODO: implement @source_avoid_type
        TODO: implement @data_support=False

        Args:
            conn: sqlite3.Connection
                Database connection
            indices: list(int)
                Indices in the annotation table
            window_ms: int
                Window size in milliseconds.
            step_ms: int
                Step size in milliseconds. Used for creating temporally translated views of the same annotation.
                If None, at most one (1) selection will be created per annotation.
            center: bool
                Align the selection window temporally with the midpoint of the annotation. If False, the temporal 
                alignment will be chosen at random (uniform distribution).
            exclusive: bool            
                If True, the selection window is not allowed to contain anything but the annotated section of data. 
                In other words, the selection window is not allowed extend beyond the start/end point of the annotation.
                In particular, this means that selections will not be created for annotations shorther than @window_ms.
                Default is False. 
            num_max: int
                Create at most this many selections.
            source_type_avoid: tuple, list(tuple)
                Only return selections that have been verified to not contain sounds with this (source,type) label.
                Note that the requirement extends to all ancestral and descendant nodes in the taxonomy tree.
            data_support: bool
                If True, selection windows are not allow to extend beyond the start/end times of the audio files in the database.
                Default is True. 
            progress_bar: bool
                Whether to display a progress bar. Default is False.
            full_path: bool
                Whether to include the full audio file paths in the output table. Default is True.

        Returns:
            sel_tbl: Pandas DataFrame
                Selection table
    """
    if source_type_avoid:
        raise NotImplementedError("@source_type_avoid not yet implemented")

    # selection timedelta
    sel_delta = timedelta(seconds=window_ms / 1.E3)
    step_delta = timedelta(seconds=step_ms / 1.E3) if step_ms is not None else None

    # retrieve annotation data
    annot_tbl = kdb.get_annotations(conn, indices)

    # append column with annotation indices
    annot_tbl["annot_id"] = indices

    # if @exclusive=True, discard all annotations shorter than @window_ms
    if exclusive:
        annot_tbl = annot_tbl[annot_tbl.duration_ms >= window_ms]

    # compute no. views of each annotation
    stepping = (step_ms is not None)
    annot_tbl = _compute_number_of_views(annot_tbl, window_ms, num_max, stepping)

    # discard annotations with 0 views
    annot_tbl = annot_tbl[annot_tbl.num_view != 0]

    # query database for audio file data
    file_ids = annot_tbl.file_id.unique()
    file_tbl = _fetch_audiofile_metadata(conn, file_ids)

    # loop over annotations
    sel_tbl = []
    num_sel = 0
    for idx,row in tqdm(annot_tbl.iterrows(), total=annot_tbl.shape[0], disable=not progress_bar):

        # compute UTC times of the views
        view_centers = _compute_view_centers(row, sel_delta, step_delta, center)

        # map UTC times to audio filename and offsets
        selection = _map_to_audiofile(row, sel_delta, view_centers, conn, file_tbl, full_path, data_support)

        if len(selection) == 0:
            continue

        # assign an ID to the selection
        selection.sel_id += num_sel

        # append the annotation index
        selection["annot_id"] = row.annot_id

        # collect selections and increment counter
        sel_tbl.append(selection)
        num_sel = selection.sel_id.iloc[-1] + 1

    # concatenate into a pandas DataFrame
    sel_tbl = pd.concat(sel_tbl, ignore_index=True)

    sel_tbl.set_index(["sel_id","filename"], inplace=True)

    # round to ms
    sel_tbl = sel_tbl.round({"start":3, "end":3})

    return sel_tbl


def _fetch_audiofile_metadata(conn, file_ids):
    """ Helper function for @create_selections.
    
        Retrieves file metadata from the database for given file indices.

        Args:
            conn: sqlite3.Connection
                Database connection
            file_ids: list(int)
                Indices of the files we want to retrieve data for.

        Returs:
            df: Pandas DataFrame
                File table 
    """
    c = conn.cursor()

    # query file data
    columns = [
        "f.id",
        "f.deployment_id",
        "f.filename",
        "f.relative_path",
        "f.sample_rate",
        "f.num_samples",
        "f.start_utc",
        "f.end_utc",
        "s.path"
    ]
    query = f"""
        SELECT 
            {','.join(columns)} 
        FROM 
            file AS f
        LEFT JOIN
            storage AS s
        ON
            f.storage_id = s.id 
        WHERE 
            f.id IN {list_to_str(file_ids)}
        """
    file_data = c.execute(query).fetchall()

    # pass data to a pandas DataFrame
    columns = [c[c.find(".") + 1 : ] for c in columns]
    file_tbl = pd.DataFrame(file_data, columns=columns)

    # rename file id column
    file_tbl.rename(columns={"id":"file_id"}, inplace=True)

    # convert to datetime
    file_tbl.start_utc = pd.to_datetime(file_tbl.start_utc, format="%Y-%m-%d %H:%M:%S.%f")

    # add duration column
    file_tbl["duration_s"] = file_tbl.apply(lambda r: r.num_samples / r.sample_rate, axis=1)

    # sort according to deployment and time, in that order
    file_tbl.sort_values(by=["deployment_id", "start_utc", "end_utc"], inplace=True)

    # set file_id as index
    file_tbl.set_index("file_id", inplace=True)

    return file_tbl


def _map_to_audiofile(row, window, view_centers, conn, file_tbl, full_path, data_support): 
    """ Helper function for @create_selections.

        Maps the selection window UTC timestamps to the 
        audio filename and within-file offset.

        Args:
            row: Pandas Series
                A single row in the annotation table
            window: datetime.timedelta
                Selection window size
            view_centers: list(datetime.datetime)
                UTC timestamp of the window midpoint
            conn: sqlite3.Connection
                Database connection
            file_tbl: pandas DataFrame
                File table returned by the :func:`_fetch_audiofile_metadata` helper function
            full_path: bool
                Whether to include the full audio file paths in the output table. 
            data_support: bool
                If True, selection windows are not allow to extend beyond the start/end times of the audio files in the database.

        Returns:
            : pandas DataFrame
                Selection; has columns: sel_id, filename, start, end
                
    """
    # selection view start/end times in seconds relative to file start
    delta = np.array(
        [((vc - 0.5 * window) - row.start_utc).total_seconds() for vc in view_centers]
    )
    start = row.start_ms / 1E3 + delta
    end = start + window.total_seconds()

    # audiofile path
    filename = file_tbl.loc[row.file_id].filename
    relative_path = file_tbl.loc[row.file_id].relative_path
    top_path = file_tbl.loc[row.file_id].path
    file_path = os.path.join(relative_path, filename)
    if full_path:
        file_path = os.path.join(top_path, file_path)

    # audio file duration
    file_duration = file_tbl.loc[row.file_id].duration_s

    # first, collect data for views that are fully within the audio file
    idx = (start >= 0) & (end <= file_duration)
    df = pd.DataFrame({"sel_id":np.arange(len(start[idx])), "filename":file_path, "start":start[idx], "end":end[idx]})

    # if all views are within the file, end here
    if np.sum(idx) == len(start):
        return df

    # otherwise, proceed to deal with views that extend beyond the file limits
    start = start[~idx]
    end = end[~idx]
    data = []
    sel_id = df.shape[0]
    for view_start,view_end in zip(start, end):
        
        view_start_utc = file_tbl.loc[row.file_id].start_utc + timedelta(seconds=view_start)
        view_end_utc = file_tbl.loc[row.file_id].start_utc + timedelta(seconds=view_end)

        file_data = _find_supporting_audiofiles(conn, row.deployment_id, view_start_utc, view_end_utc, full_path)

        if len(file_data) == 0:
            msg = f"Could not find supporting audio file for selection view starting at {view_start_utc}"\
                + f" and ending at {view_end_utc} for Deployment {row.deployment_id}"
            warnings.warn(msg, UserWarning)
            continue

        segments = []
        tot_duration = 0
        tot_gap = 0
        prev_file_end_utc = None
        for _,f in file_data.iterrows():
            segment_start_utc = max(view_start_utc, f.start_utc)
            segment_start = (segment_start_utc - f.start_utc).total_seconds()

            segment_end_utc = min(view_end_utc, f.end_utc)
            segment_end = (segment_end_utc - f.start_utc).total_seconds()

            segments += [[sel_id, f.path, segment_start, segment_end]]

            if prev_file_end_utc is not None:
                tot_gap += (f.start_utc - prev_file_end_utc).total_seconds()
    
            tot_duration += segment_end - segment_start
            prev_file_end_utc = f.end_utc

        # expand at beginning to account for inter-file gaps
        segments[0][2] -= tot_gap
        tot_duration += tot_gap
        tot_duration = np.round(tot_duration, 4)
        
        if data_support and (tot_duration < window.total_seconds() or segments[0][2] < 0):
            # discard the view, if there is insufficient data support
            continue
    
        elif not data_support:
            raise NotImplementedError("@data_support=False not yet implemented")

        data += segments
        sel_id += 1  

    if len(data) > 0:
        df_beyond = pd.DataFrame(data, columns=["sel_id", "filename", "start", "end"])
        df = pd.concat([df, df_beyond])
    
    return df


def _find_supporting_audiofiles(conn, deployment_id, start_utc, end_utc, full_path):
    """ Helper function for @create_selections.

        Searches the database for audiofiles that overlap temporally with a given time window.

        Args:
            conn: sqlite3.Connection
                Database connection
            deployment_id: int
                Deployment index
            start_utc, end_utc: str
                UTC start/end time in Korus standard format yyyy-mm-dd HH:MM:SS.sss
            full_path: bool
                Whether to return the full audio path

        Args:
            df: Pandas DataFrame
                File table with columns: path,start_utc",end_utc
    """
    c = conn.cursor()

    query = f"""
        SELECT
            f.filename,
            f.relative_path,
            s.path,
            f.start_utc,
            f.end_utc
        FROM 
            file AS f
        LEFT JOIN
            storage AS s
        ON 
            f.storage_id = s.id
        WHERE
            deployment_id = {deployment_id}
            AND (start_utc < '{end_utc}' AND end_utc > '{start_utc}')
    """
    rows = c.execute(query).fetchall()

    data = []
    for row in rows:
        filename = row[0]
        relative_path = row[1]
        top_path = row[2]
        file_path = os.path.join(relative_path, filename)
        if full_path:
            file_path = os.path.join(top_path, file_path)

        file_start_utc = datetime.strptime(row[3], "%Y-%m-%d %H:%M:%S.%f")
        file_end_utc = datetime.strptime(row[4], "%Y-%m-%d %H:%M:%S.%f")

        data.append((file_path, file_start_utc, file_end_utc))

    return pd.DataFrame(data, columns=["path", "start_utc", "end_utc"])


def _compute_view_centers(row, window, step, center):
    """ Helper function for @create_selections.

        Computes the temporal midpoints of the view(s) created of each annotation.

        Args:
            row: Pandas Series
                A single row in the annotation table
            window: datetime.timedelta
                Selection window size
            step: datetime.timedelta
                Step size
            center: bool
                Whether the view(s) should be centered on the annotation.

        Args:
            view_centers: list(datetime.datetime)
                Temporal midpoint of each view
    """
    annot_start = row.start_utc
    annot_delta = timedelta(seconds=row.duration_ms / 1.E3)
    annot_end = annot_start + annot_delta
    annot_center = annot_start + 0.5 * annot_delta

    # wide/narrow annotation
    is_wide = (annot_delta > window)

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
            nb = (view_center - 0.5 * window - annot_start).total_seconds() / step.total_seconds()
        else:
            nb = (view_center + 0.5 * window - annot_end).total_seconds() / step.total_seconds()

        nb = np.floor(nb).astype(int)

        # step forward in time
        if is_wide:
            nf = (annot_end - view_center - 0.5 * window).total_seconds() / step.total_seconds()
        else:
            nf = (annot_start - view_center + 0.5 * window).total_seconds() / step.total_seconds()

        nf = np.floor(nf).astype(int)

        no_steps = np.arange(start=-nb, stop=nf+1, step=1)

        # if the no. selection windows is capped and we have too many selections, 
        # randomly subsample as many as we need
        if row.num_view > 1 and row.num_view < len(no_steps):
            no_steps = np.random.choice(no_steps, size=row.num_view, replace=False)

        # midpoint(s) of the selection windows (datetime objects) 
        view_centers = [view_center + i * step for i in no_steps]

    return view_centers


def _compute_number_of_views(df, window_ms, num_max, stepping):
    """ Helper function for @create_selections.

        Computes the number of selections to be created from each annotation, 
        also referred to as the number of 'views'.

        The result is stored in a column named 'num_view' which is added to the 
        input table.

        Note: If @num_max is None (i.e. no limit) @num_view is set to -1.

        Args:
            df: Pandas DataFrame
                Annotation table
            window_ms: int
                Window size in milliseconds.
            num_max: int
                Create at most this many selections.
            stepping: bool
                Whether multiple, time-translated views of each annotation are to 
                be created.

        Args:
            df: Pandas DataFrame
                Annotation table with added column 'num_view'
                
    """
    df["num_view"] = -1 if stepping else 1

    if num_max is None:
        return df

    # if the total number of selections is capped, and stepping is enabled, 
    # make the number of views of each annotation approx. proportional to 
    # max(@window_ms, row.duration_ms)
    if stepping:
        df.num_view = df.duration_ms.apply(lambda x: max(x, window_ms))
        df.num_view *= num_max / df.num_view.sum()

        def round_fcn(val, rndm):
            val_floor = np.floor(val)
            return val_floor + ((val - val_floor) > rndm)

        df["rand"] = np.random.rand(df.shape[0])
        df.num_view = df.apply(lambda r: round_fcn(r.num_view, r.rand), axis=1).astype("int")
        df.drop(columns="rand", inplace=True)

    # if stepping is disabled, each annotation contributes 0 or 1 selection, randomly sampled
    elif num_max < df.shape[0]:
        df.num_view = 0
        idx = np.random.choice(np.arange(df.shape[0]), size=num_max, replace=False)
        df.loc[idx, "num_view"] = 1

    return df
