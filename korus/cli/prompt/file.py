from datetime import datetime
from korus.audio import collect_audiofile_metadata


AUDIO_FORMATS = ["wav", "flac", "ogg", "mp3"]


def from_txt(dir_path, timestamp_parser):
    pass


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
    return collect_audiofile_metadata(
        path=dir_path,
        ext=AUDIO_FORMATS,
        timestamp_parser=timestamp_parser,
        progress_bar=True,
        inspect_files=False,
    )
