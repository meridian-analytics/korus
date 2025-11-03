import os
import pytest
from datetime import datetime
import pandas as pd
import korus.audio as ka


current_dir = os.path.dirname(os.path.realpath(__file__))
path_to_assets = os.path.join(current_dir, "assets")
path_to_tmp = os.path.join(path_to_assets, "tmp")


def test_find_files_dir():
    """Check that we can find files in a directory"""
    path = os.path.join(path_to_assets, "files")
    f = ka.find_files(path)
    f.sort()
    assert f == ["a.txt", "b.wav"]
    f = ka.find_files(path, subdirs=True)
    f.sort()
    assert f == [
        "a.txt",
        "b.wav",
        "more-files/c.flac",
        "more-files/d.wav",
        "more-files/e.txt",
    ]
    f = ka.find_files(path, substr="wav", subdirs=True)
    f.sort()
    assert f == ["b.wav", "more-files/d.wav"]


def test_find_files_tar():
    """Check that we can find files in a zipped tar archive"""
    path = os.path.join(path_to_assets, "zipped-files.tar.gz")
    f = ka.find_files(path, subdirs=True)
    f.sort()
    assert f == [
        "a.txt",
        "b.wav",
        "more-files/c.flac",
        "more-files/d.wav",
        "more-files/e.txt",
    ]
    f = ka.find_files(path, substr="wav")
    f.sort()
    assert f == ["b.wav"]
    f = ka.find_files(path, substr="wav", subdirs=True)
    f.sort()
    assert f == ["b.wav", "more-files/d.wav"]
    f = ka.find_files(path, substr="wav", tar_path="/more-files")
    f.sort()
    assert f == ["d.wav"]
    f = ka.find_files(path, substr="wav", tar_path="non-existing-path")
    f.sort()
    assert f == []


@pytest.mark.parametrize(
    "rel_path", ["timestamped-audiofiles", "zipped-timestamped-audiofiles.tar.gz"]
)
def test_collect_audiofile_metadata(rel_path):
    """Check that we can extract metadata from audio files both stored
    within a regular directory and within a zipped tar archive"""

    def timestamp_parser(x):
        p = x.rfind("/") + 1
        return datetime.strptime(x[p : p + 19], "%Y%m%dT%H%M%S.%f")

    path = os.path.join(path_to_assets, rel_path)

    # by default, only search for WAV files
    df = ka.collect_audiofile_metadata(path)
    assert len(df) == 0

    # search for FLAC files
    df = ka.collect_audiofile_metadata(path, ext="FLAC")
    # compare to expected result
    answ_path = os.path.join(
        path_to_assets, "timestamped-audiofiles/df-no-timestamps.csv"
    )
    answ = pd.read_csv(answ_path).astype({"relative_path": "str"})
    pd.testing.assert_frame_equal(df, answ)

    # search for FLAC files with timestamp parser
    df = ka.collect_audiofile_metadata(
        path, ext="FLAC", timestamp_parser=timestamp_parser
    )
    # compare to expected result
    answ_path = os.path.join(path_to_assets, "timestamped-audiofiles/df-timestamps.csv")
    answ = pd.read_csv(answ_path).astype({"relative_path": "str"})
    answ.start_utc = pd.to_datetime(answ.start_utc, format="ISO8601")
    answ.end_utc = pd.to_datetime(answ.end_utc, format="ISO8601")
    pd.testing.assert_frame_equal(df, answ)

    # search with time constraints and by_date=True
    earliest_start_utc = datetime(2024, 6, 30, 12, 0, 0)
    df = ka.collect_audiofile_metadata(
        path,
        ext="FLAC",
        timestamp_parser=timestamp_parser,
        earliest_start_utc=earliest_start_utc,
        by_date=True,
    )
    # compare to expected result
    answ_2024 = answ[answ.start_utc >= earliest_start_utc].reset_index(drop=True)
    pd.testing.assert_frame_equal(df, answ_2024)

    # search on subset of files using filenames
    subset_filename = [
        "20230701T130007.900_12Hz_9s.flac",
        "20240701T120000.000_12Hz_10s.flac",
    ]
    df = ka.collect_audiofile_metadata(
        path,
        ext="FLAC",
        timestamp_parser=timestamp_parser,
        subset_filename=subset_filename,
        by_date=True,
    )
    answ = answ[answ.filename.isin(subset_filename)]
    pd.testing.assert_frame_equal(df, answ)
