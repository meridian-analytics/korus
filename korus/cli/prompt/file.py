import os
import inquirer
from datetime import timedelta
from korus.audio import collect_audiofile_metadata, extract_num_samples_and_samplerate
import korus.cli.prompt.prompt as prompt
import korus.cli.text as txt


AUDIO_FORMATS = ["wav", "flac", "ogg", "mp3"]


def from_txt(dir_path, timestamp_parser):
    txt_path = prompt.enter_path(msg="Enter path")
    with open(txt_path, "r") as f:
        filenames = [line.rstrip() for line in f]

    return from_filename(dir_path, timestamp_parser, filenames)


def from_csv(dir_path, timestamp_parser):
    pass


def from_raven(dir_path, timestamp_parser):
    pass


def from_console(dir_path, timestamp_parser):
    pass


def from_time_range(dir_path, timestamp_parser, by_date):

    # TODO: prompt user for start & end times
    start = None
    end = None

    return collect_audiofile_metadata(
        path=dir_path,
        ext=AUDIO_FORMATS,
        timestamp_parser=timestamp_parser,
        progress_bar=True,
        inspect_files=False,
        by_date=by_date,
        earliest_start_utc=start,
        latest_start_utc=end,
    )


def from_filename(
    dir_path,
    timestamp_parser,
    filename: str | list[str] = None,
):
    if isinstance(filename, str):
        filename = [filename]

    # search for files and parse timestamps
    df = collect_audiofile_metadata(
        path=dir_path,
        ext=AUDIO_FORMATS,
        timestamp_parser=timestamp_parser,
        subset_filename=filename,
        progress_bar=True,
        inspect_files=False,
    )

    # missing files, if any
    not_found = [fname for fname in filename if fname not in df.filename.values]

    print(df)

    msg = f"Found {len(df)} of {len(filename)} files"
    msg = (
        txt.info(msg, newline=False)
        if len(not_found) == 0
        else txt.warn(msg, newline=False)
    )
    print(msg)

    if len(not_found) > 0:
        print(txt.warn("The following files were not found:"))
        for fname in not_found:
            print(fname)

    if len(df) == 0:
        return

    # choices: message -> index
    choices = dict()
    default = list()
    for idx, row in df.iterrows():
        choice_str = f"{row.filename}"
        if row.start_utc is not None:
            choice_str += f" | {row.start_utc}"
            default.append(choice_str)

        choices[choice_str] = idx

    msg = "Select the files you wish to add to the database, or hit Ctrl+C to abort"
    answers = inquirer.checkbox(
        msg,
        choices=choices.keys(),
        default=default,
    )

    # apply selection
    indices = [choices[a] for a in answers]
    df = df.loc[indices]

    # extract durations and sampling rates
    num_samples, sample_rate = extract_num_samples_and_samplerate(
        path=[
            os.path.join(row.relative_path, row.filename) for _, row in df.iterrows()
        ],
        base_path=dir_path,
        progress_bar=True,
    )
    df["num_samples"] = num_samples
    df["sample_rate"] = sample_rate

    # end_utc
    if "start_utc" in df.columns:
        df["t_end"] = df.apply(
            lambda r: r.t + timedelta(seconds=float(r.num_samples) / r.sample_rate),
            axis=1,
        )
        df["end_utc"] = df.t_end.dt.strftime("%Y-%m-%d %H:%M:%S.%f")

    return df
