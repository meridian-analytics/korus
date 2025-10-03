import os
import shutil
import logging
import soundfile as sf
import pandas as pd
import tarfile
from datetime import datetime, timedelta, date
from tqdm import tqdm


def group_by_date(filenames: list[str], timestamp_parser: callable):
    """Helper function for grouping audiofiles by their start date.

    Args:
        filenames: list[str]
            Filenames or paths
        timestamp_parser: callable
            Function that takes a string as input and returns a datetime.datetime object.

    Returns:
        grouped: dict[datetime.date, list[str]]
            Dictionary mapping of dates in the form %Y%m%d to filenames.
            OBS: If timestamp parsing fails for ANY of the files, ALL files are grouped together with null key.
    """
    # attempt to parse timestamps
    indices, timestamps = parse_timestamp(
        filenames, timestamp_parser, progress_bar=False
    )

    if len(indices) < len(filenames):
        return {None: filenames}

    # group according to date
    grouped = {}
    for i, t in zip(indices, timestamps):
        date_key = t.date()
        fname = filenames[i]
        grouped[date_key] = grouped.get(date_key, []) + [fname]

    return grouped


def collect_audiofile_metadata(
    path: str,
    ext: str | list[str] = "WAV",
    timestamp_parser: callable = None,
    earliest_start_utc: datetime | date = None,
    latest_start_utc: datetime | date = None,
    subset: str | list[str] = None,
    subset_filename: str | list[str] = None,
    tar_path: str = "",
    progress_bar: bool = False,
    by_date: bool = False,
    inspect_files: bool = True,
    tmp_path: str = "./korus-tmp",
):
    """Collect metadata records for all audio files in a specified directory.

    In order to extract timestamps embedded in the filenames, you must specify a
    parser function using the @timestamp_parser argument. This function must take
    the relative path to the audio file as input (as a string) and return the
    UTC start time of the file (as a datetime.datetime object).

    Args:
        path: str
            Path to the directory or tar archive where the audio files are stored.
        ext: str | list[str]
            Audio file extension(s). Default is WAV.
        timestamp_parser: callable
            Function that takes a string as input and returns a datetime.datetime object.
        earliest_start_utc: datetime.datetime | datetime.date
            Only consider files starting at or after this UTC time.
        latest_start_utc: datetime.datetime | datetime.date
            Only consider files starting at or before this UTC time.
        subset: str | list(str)
            Paths relative to the top directory given by the `path` argument. Use
            this argument to restrict attention to a subset of the files.
        subset_filename: str | list(str)
            Same as `subset` except only requires the filename(s) to be specified.
        tar_path: str
            Path within tar archive. Only relavant if @path points to a tar archive.
        progress_bar: bool
            Display progress bar. Default is False.
        by_date: bool
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
    if isinstance(subset_filename, str):
        subset_filename = [subset_filename]

    if isinstance(ext, str):
        ext = [ext]

    if isinstance(earliest_start_utc, date):
        earliest_start_utc = datetime.combine(earliest_start_utc, datetime.min.time())

    if isinstance(latest_start_utc, date):
        latest_start_utc = datetime.combine(latest_start_utc, datetime.max.time())

    # user has specified a list of filenames:
    if subset_filename is not None:
        if by_date and timestamp_parser:
            # group filenames by date
            grouped = group_by_date(subset_filename, timestamp_parser)

            # recursive call to collect audiofile metadata, one date-stamped subfolder at the time
            df = [
                collect_audiofile_metadata(
                    path=path,
                    ext=ext,
                    timestamp_parser=timestamp_parser,
                    earliest_start_utc=_date,
                    latest_start_utc=_date,
                    tar_path=tar_path,
                    progress_bar=progress_bar,
                    by_date=True,
                    inspect_files=inspect_files,
                )
                for _date, filenames in grouped.items()
                if len(filenames) > 0
            ]
            df = pd.concat(df, ignore_index=True)

            # only keep files with matching filenames
            df = df[df.filename.isin(subset_filename)]

            return df

        else:
            # search for files based on filename
            subset = find_files(
                path,
                substr=subset_filename,
                subdirs=True,
                progress_bar=progress_bar,
            )

    # both start and end time must be specified to allow date-restricted search
    search_by_date = (
        by_date and earliest_start_utc is not None and latest_start_utc is not None
    )
    if search_by_date:
        earliest_date = earliest_start_utc.date()
        latest_date = latest_start_utc.date()

    # whether base path points to a tar archive instead of a directory
    is_tar = os.path.isfile(path) and tarfile.is_tarfile(path)

    # rename
    rel_path = subset

    if rel_path is None:
        if search_by_date:
            sub_folders = []
            _date = earliest_date
            while _date <= latest_date:
                date_str = _date.strftime("%Y%m%d")
                sub_folders.append(date_str)
                _date += timedelta(days=1)
        else:
            sub_folders = [""]

        rel_path = []
        for sub_folder in sub_folders:
            if is_tar:
                kwargs = {"path": path, "tar_path": os.path.join(tar_path, sub_folder)}

            else:
                kwargs = {"path": os.path.join(path, sub_folder)}

            substr = [x.lower() for x in ext] + [x.upper() for x in ext]

            file_paths = find_files(
                **kwargs,
                substr=substr,
                subdirs=True,
                progress_bar=progress_bar,
            )
            rel_path += [
                os.path.join(sub_folder, file_path) for file_path in file_paths
            ]

    if isinstance(rel_path, str):
        rel_path = [rel_path]

    df = pd.DataFrame({"rel_path": rel_path})

    def get_ext(x):
        """Helper function for parsing the file extension"""
        p = x.rfind(".")
        if p == -1:
            return ""
        else:
            return x[p + 1 :].upper()

    df["format"] = df["rel_path"].apply(lambda x: get_ext(x))

    logging.debug(f"Found {len(df)} {ext} files in {path}")

    # parse timestamps
    if timestamp_parser is not None:
        indices, timestamps = parse_timestamp(rel_path, timestamp_parser, progress_bar)

        df["start_utc"] = None
        df.start_utc = pd.to_datetime(df.start_utc)
        df.loc[indices, "start_utc"] = timestamps

        logging.debug(f"Successfully parsed {len(indices)} of {len(df)} timestamps")

        # optionally, apply time cuts
        if earliest_start_utc is not None:
            df = df[df.start_utc >= earliest_start_utc]
        if latest_start_utc is not None:
            df = df[df.start_utc <= latest_start_utc]

    # inspect files to obtain no. samples and sampling rate
    if inspect_files:
        num_samples, sample_rate = extract_num_samples_and_samplerate(
            path=df.rel_path,
            base_path=path,
            tmp_path=tmp_path,
            progress_bar=progress_bar,
        )

        df["num_samples"] = num_samples
        df["sample_rate"] = sample_rate

        # end_utc
        if "start_utc" in df.columns:
            df["end_utc"] = df.apply(
                lambda r: r.start_utc
                + timedelta(seconds=float(r.num_samples) / r.sample_rate),
                axis=1,
            )

    # rel_path -> filename, relative_path
    df["filename"] = df["rel_path"].apply(lambda x: os.path.basename(x))
    df["relative_path"] = df["rel_path"].apply(lambda x: os.path.dirname(x))

    # drop unneccesary columns
    df.drop(columns=["rel_path"], inplace=True)

    df.reset_index(drop=True, inplace=True)
    return df


def extract_num_samples_and_samplerate(
    path: str | list[str],
    base_path: str = "",
    tmp_path: str = "./korus-tmp",
    progress_bar: bool = False,
):
    """Obtain duration and samplerate of a set of audio files

    TODO: implement error handling; return args should include which
        files were succesfully read and which could not be read

    Args:
        path: str | list[str]
            Relative paths including filename to the audio files
        base_path: str
            Top directory
        tmp_path: str
            If the audio files are stored in tar archive, and @inspect_files is True, audio files
            will be extracted to this folder temporarily to allow the file size and sampling rate
            to be determined.
        progress_bar: bool
            Display progress bar. Default is False.

    Returns:
        num_samples: list
            Number of samples per file
        sample_rate: list
            Samplerate in samples/s.
    """
    if progress_bar:
        print("Determining sampling rates and file sizes ...")

    # whether base path points to a tar archive instead of a directory
    is_tar = os.path.isfile(base_path) and tarfile.is_tarfile(base_path)

    if isinstance(path, str):
        path = [path]

    # open tar archive, and create temporary folder for extracting audio files
    if is_tar:
        tar = tarfile.open(base_path)
        shutil.rmtree(tmp_path, ignore_errors=True)
        os.makedirs(tmp_path)

    # loop over files and get no. samples and sampling rate for each one
    num_samples, sample_rate = [], []
    for x in tqdm(path, disable=not progress_bar):
        if is_tar:
            member = tar.getmember(x)
            tar.extract(member, path=tmp_path)
            full_path = os.path.join(tmp_path, x)

        else:
            full_path = os.path.join(base_path, x)

        n, sr = read_num_samples_and_samplerate(full_path)
        num_samples.append(n)
        sample_rate.append(sr)

        if is_tar:
            os.remove(full_path)

    if is_tar:
        tar.close()
        shutil.rmtree(tmp_path, ignore_errors=True)

    return num_samples, sample_rate


def read_num_samples_and_samplerate(path):
    """Determine the number of samples and sampling rate of a given audio file.

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


def find_files(path, substr=None, subdirs=False, tar_path="", progress_bar=False):
    """Search a directory or tar archive for files with a specified sequence of characters in their path.

    Args:
        path: str
            Path to directory or tar archive file
        substr: str | list(str)
            Search for files that have this string/these strings in their path.
        subdirs: bool
            If True, also search all subdirectories.
        tar_path: str
            Path within tar archive. Only relavant if `path` points to a tar archive.
        progress_bar: bool
            Display progress bar. Default is False.

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
    if progress_bar:
        print("Listing all files in directory ...")

    all_files = []
    if is_tar:
        with tarfile.open(path) as tar:
            for member in tqdm(tar.getmembers(), disable=not progress_bar):
                if member.isreg():  # skip if not a file
                    mem_path = member.name

                    if is_tar:
                        # skip files not in the specified directory within the tar archive
                        if mem_path[: len(tar_path)] != tar_path:
                            continue

                        # drop top path within tar archive
                        mem_path = mem_path[len(tar_path) :]

                    # skip files in sub directories
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
    if progress_bar:
        print("Filtering and sorting files ...")

    files = []
    for f in tqdm(all_files, disable=not progress_bar):
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
    """Parses timestamps from a list of strings using a user-specified function.

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
