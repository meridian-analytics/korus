import os
import shutil
import logging
import soundfile as sf
import pandas as pd
import numpy as np
import tarfile
import warnings
from datetime import datetime, timedelta
from tqdm import tqdm


def list_to_str(l):
    """ Transform a list to a string, suitably formatted for forming SQLite queries.
    
        Args:
            l: list or numpy array
                List of values

        Returns:
            : str
                String
    """
    if not isinstance(l, (list, np.ndarray)):
        l = [l]
    return "(" + ",".join([f"'{x}'" for x in l]) + ")"


def collect_audiofile_metadata(
    path, 
    ext="WAV", 
    timestamp_parser=None,
    earliest_start_utc=None,
    latest_start_utc=None,
    subset=None,
    tar_path="",
    progress_bar=False,
    date_subfolder=False,
    inspect_files=True,
    tmp_path="./korus-tmp",
):
    
    """ Collect metadata records for all audio files in a specified directory.

    In order to extract timestamps embedded in the filenames, you must specify a 
    parser function using the @timestamp_parser argument. This function must take 
    the relative path to the audio file as input (as a string) and return the 
    UTC start time of the file (as a datetime.datetime object).
    
    Args:
        path: str
            Path to the directory or tar archive where the audio files are stored.
        ext: str
            Audio file extension. Default is WAV.
        timestamp_parser: callable
            Function that takes a string as input and returns a datetime.datetime object. 
        earliest_start_utc: datetime.datetime
            Only consider files starting at or after this UTC time.
        latest_start_utc: datetime.datetime
            Only consider files starting at or before this UTC time.
        subset: str, list(str)
            File paths relative to the top directory given by the @path argument. Use 
            this argument to restrict attention to a subset of the files.
        tar_path: str
            Path within tar archive. Only relavant if @path points to a tar archive.
        progress_bar: bool
            Display progress bar. Default is False.
        date_subfolder: bool
            If audio files are organized in date-stamped subfolders with format yyyymmdd, 
            and both the earliest and latest start time have been specified, this argument 
            can be used to restrict the search space to only the relevant subfolders. 
            Default is False.
        inspect_files: bool
            Inspect files to obtain no. samples and sampling rate. If False, the returned 
            metadata table does not have the columns `num_samples`, `sample_rate`, and `end_utc`. 
            Default is True.
        tmp_path: str
            If the audio files are stored in tar archive, and @inspect_files is True, audio files 
            will be extracted to this folder temporarily to allow the file size and sampling rate 
            to be determined.

    Returns:
        df: pandas DataFrame
            Metadata table

    Examples:
    """
    is_tar = os.path.isfile(path) and tarfile.is_tarfile(path)

    rel_path = subset

    if rel_path is None:
        if date_subfolder and earliest_start_utc and latest_start_utc:
            sub_folders = []
            date = earliest_start_utc.date()
            while date <= latest_start_utc.date():
                date_str = date.strftime("%Y%m%d")
                sub_folders.append(date_str)
                date += timedelta(days=1)         
        else:
            sub_folders = [""]

        rel_path = []
        for sub_folder in sub_folders:
            if is_tar:
                kwargs = {
                    "path": path,
                    "tar_path": os.path.join(tar_path, sub_folder)
                }

            else:
                kwargs = {"path": path}

            file_paths = find_files(**kwargs, substr=[ext.lower(), ext.upper()], subdirs=True)       
            rel_path += [os.path.join(sub_folder, file_path) for file_path in file_paths]

    if isinstance(rel_path, str):
        rel_path = [rel_path]

    df = pd.DataFrame({"rel_path": rel_path})

    df["format"] = ext.upper()

    logging.debug(f"Found {len(df)} {ext} files in {path}")

    # parse timestamps
    if timestamp_parser is not None:
        indices, timestamps = parse_timestamp(rel_path, timestamp_parser, progress_bar)

        df["t"] = None
        df.t = pd.to_datetime(df.t)
        df.loc[indices, "t"] = timestamps

        df["start_utc"] = ""
        df.loc[indices, "start_utc"] = df.t.loc[indices].dt.strftime("%Y-%m-%d %H:%M:%S.%f")

        logging.debug(f"Successfully parsed {len(indices)} of {len(df)} timestamps")

        # optionally, apply time cuts
        if earliest_start_utc is not None:
            df = df[df.t >= earliest_start_utc]
        if latest_start_utc is not None:
            df = df[df.t <= latest_start_utc]

    # inspect files to obtain no. samples and sampling rate
    if inspect_files:
        if progress_bar:
            print("Determining sampling rates and file sizes ...")

        # open tar archive, and create temporary folder for extracting audio files
        if is_tar:
            tar = tarfile.open(path)
            shutil.rmtree(tmp_path, ignore_errors=True)
            os.makedirs(tmp_path)

        # loop over files and get no. samples and sampling rate for each one
        num_samples, sample_rate = [], []
        for _,row in tqdm(df.iterrows(), total=df.shape[0], disable=not progress_bar):
            if is_tar:
                member = tar.getmember(row.rel_path)
                tar.extract(member, path=tmp_path)
                full_path = os.path.join(tmp_path, row.rel_path)

            else:
                full_path = os.path.join(path, row.rel_path)
    
            n, sr = get_num_samples_and_rate(full_path)
            num_samples.append(n)
            sample_rate.append(sr)

            if is_tar:
                os.remove(full_path)           

        if is_tar:
            tar.close()
            shutil.rmtree(tmp_path, ignore_errors=True)

        df["num_samples"] = num_samples
        df["sample_rate"] = sample_rate

        # end_utc
        if "start_utc" in df.columns:
            df["t_end"] = df.apply(lambda r: r.t + timedelta(seconds=float(r.num_samples) / r.sample_rate), axis=1)
            df["end_utc"] = df.t_end.dt.strftime("%Y-%m-%d %H:%M:%S.%f")

    # rel_path -> filename, relative_path
    df["filename"] = df["rel_path"].apply(lambda x: os.path.basename(x))
    df["relative_path"] = df["rel_path"].apply(lambda x: os.path.dirname(x))
    
    # drop unneccesary columns
    drop_cols = ["rel_path"]
    if "t" in df.columns:
        drop_cols += ["t"]
    if "t_end" in df.columns:
        drop_cols += ["t_end"]

    df.drop(columns=drop_cols, inplace=True)

    df.reset_index(drop=True, inplace=True)
    return df


def get_num_samples_and_rate(path):
    """ Determine the number of samples and sampling rate of a given audio file.

        Args:
            path: str
                Full path to the audio file

        Returns:
            : int, int
                No. samples and sampling rate in Hz
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} not found.")

    try:
        with sf.SoundFile(path, "r") as f:
            return f.frames, f.samplerate

    except:
        raise IOError(f"{path} could not be read.")


def find_files(path, substr=None, subdirs=False, tar_path=""):
    """Search a directory or tar archive for files with a specified sequence of characters in their path.

    Args:
        path: str
            Path to directory or tar archive file
        substr: str or list(str)
            Search for files that have this string/these strings in their path.
        subdirs: bool
            If True, also search all subdirectories.
        tar_path: str
            Path within tar archive. Only relavant if @path points to a tar archive.

    Returns:
        files: list (str)
            Alphabetically sorted list of relative file paths

    Examples:
    """
    # strip leading slash from @tar_path
    if len(tar_path) > 0 and tar_path[0] == "/":
        tar_path = tar_path[1:]

    # add trailing slash to @tar_path
    if len(tar_path) > 0 and tar_path[-1] != "/":
        tar_path += "/"

    # determine if audio files are stored in a directory or a tar archive
    is_tar = os.path.isfile(path) and tarfile.is_tarfile(path)

    if isinstance(substr, str):
        substr = [substr]

    # find all files
    all_files = []
    if is_tar:
        with tarfile.open(path) as tar:
            for member in tar.getmembers():
                if member.isreg(): #skip if not a file
                    mem_path = member.name

                    if is_tar:
                        #skip files not in the specified directory within the tar archive
                        if mem_path[:len(tar_path)] != tar_path:
                            continue

                        #drop top path within tar archive
                        mem_path = mem_path[len(tar_path):]
                        
                    #skip files in sub directories
                    if not subdirs and "/" in mem_path: 
                        continue

                    all_files.append(mem_path)

    else:
        if subdirs:
            for dirpath, _, files in os.walk(path):
                all_files += [
                    os.path.relpath(os.path.join(dirpath, f), path) for f in files
                ]
        else:
            all_files = os.listdir(path)

    # filter
    files = []
    for f in all_files:
        # skip directories
        if not is_tar and os.path.isdir(os.path.join(path, f)):
            continue

        # filter for substring(s)
        if substr is None:
            files.append(f)

        else:
            for ss in substr:
                if ss in f:
                    files.append(f)
                    break

    # sort alphabetically
    files.sort()

    return files


def parse_timestamp(x, timestamp_parser, progress_bar=False):
    """ Parses timestamps from a list of strings using a user-specified function.

        Args:
            x: list(str)
                Strings to be parsed
            timestamp_parser: function
                Function that takes a single str as input and returns a datetime object
            progress_bar: bool
                Display progress bar. Default is False.

        Returns:
            indices: list(int)
                Indices of the strings that were successfully parsed 
            timestamps: list(datetime)
                Parsed datetime values

        Examples:
    """
    indices, timestamps = [], []

    if progress_bar:
        print("Parsing timestamps ...")

    for i, s in tqdm(enumerate(x), total=len(x), disable=not progress_bar):
        try:
            dt = timestamp_parser(s)
            indices.append(i)
            timestamps.append(dt)
        except:
            continue

    return indices, timestamps


