from korus.audio import collect_audiofile_metadata


AUDIO_FORMATS = ["wav", "flac", "ogg", "mp3"]


def files_from_txt(dir_path, timestamp_parser):
    pass


def files_from_csv(dir_path, timestamp_parser):
    pass


def files_from_raven(dir_path, timestamp_parser):
    pass


def files_from_console(dir_path, timestamp_parser):
    pass


def files_from_time_range(dir_path, timestamp_parser, by_date):
    df = collect_audiofile_metadata(
        path=dir_path,
        ext=AUDIO_FORMATS,
        timestamp_parser=timestamp_parser,
        progress_bar=True,
        inspect_files=False,
        date_subfolder=by_date,
    )


def all_files(dir_path, timestamp_parser):
    df = collect_audiofile_metadata(
        path=dir_path,
        ext=AUDIO_FORMATS,
        timestamp_parser=timestamp_parser,
        progress_bar=True,
        inspect_files=False,
    )
