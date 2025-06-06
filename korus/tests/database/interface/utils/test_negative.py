import pytest
import pandas as pd
from datetime import datetime, timedelta
from korus.database.interface.utils.negative import (
    MonoTimePeriod,
    StereoTimePeriod,
    find_unannotated_periods,
)


def test_find_unannotated_periods_single_deployment_single_channel():
    t0 = datetime(2022, 12, 2)

    # three 1-minute long files, spaced apart by 0s and 1s
    files = pd.DataFrame(
        {
            "deployment_id": [0, 0, 0],
            "file_id": [10, 11, 12],
            "channel": [[0], [0], [0]],
            "start_utc": [
                t0,
                t0 + timedelta(minutes=1),
                t0 + timedelta(minutes=2, seconds=1),
            ],
            "end_utc": [
                t0 + timedelta(minutes=1),
                t0 + timedelta(minutes=2),
                t0 + timedelta(minutes=3, seconds=1),
            ],
        }
    )

    # two short annotations in the 1st file
    annots = pd.DataFrame(
        {
            "deployment_id": [0, 0],
            "file_id": [10, 10],
            "channel": [0, 0],
            "start": [10.0, 20.0],
            "duration": [3.0, 4.0],
        }
    )

    negatives = find_unannotated_periods(files, annots)

    expected = """   deployment_id  file_id file_id_list  channel  duration  start
0              0       10         [10]        0      10.0    0.0
1              0       10         [10]        0       7.0   13.0
2              0       10     [10, 11]        0      96.0   24.0
3              0       12         [12]        0      60.0    0.0"""
    assert negatives.to_string() == expected


def test_stereo_time_period():
    sp = StereoTimePeriod(0, 0.1)

    start_0 = datetime(2022, 12, 2)
    end_0 = start_0 + timedelta(minutes=1)

    # new file, channel 3
    p = sp.new_file(
        channel=3,
        file_id=0,
        file_start_utc=start_0,
        file_end_utc=end_0,
    )
    assert p.channel == 3
    assert not p.has_ended
    assert p.start_utc == start_0

    # new file, channel 5
    p = sp.new_file(
        channel=5,
        file_id=0,
        file_start_utc=start_0,
        file_end_utc=end_0,
    )
    assert p.channel == 5
    assert not p.has_ended
    assert p.start_utc == start_0

    # new annotation, channel 3
    # start 3 seconds into the file
    # ends 5 seconds past the end of the file
    start = start_0 + timedelta(seconds=3)
    end = end_0 + timedelta(seconds=5)
    p = sp.new_annotation(5, start, end)
    assert p.has_ended
    assert p.end_utc == start

    # new file, channel 5
    # starts just 10 ms after the previous file
    p = sp.new_file(
        channel=5,
        file_id=1,
        file_start_utc=end_0 + timedelta(seconds=0.01),
        file_end_utc=end_0 + timedelta(minutes=1),
    )
    # new period should start where annotation ended
    assert p.channel == 5
    assert not p.has_ended
    assert p.start_utc == end


def test_mono_time_period_new_annotation_before():

    start_0 = datetime(2022, 12, 2)
    end_0 = start_0 + timedelta(minutes=1)

    # create mono time period
    kwargs = dict(
        deployment_id=0,
        file_ids=[0],
        channel=0,
        start_utc=start_0,
        file_end_utc=end_0,
        max_file_gap=0.1,
    )
    p = MonoTimePeriod(**kwargs)
    assert not p.has_ended

    # new annotation starting *before* period
    start_1 = start_0 + timedelta(seconds=-3)
    end_1 = start_1 + timedelta(seconds=4)
    p.new_annotation(start_1, end_1)
    assert not p.has_ended
    assert p.start_utc == end_1


def test_mono_time_period_new_annotation_after():

    start_0 = datetime(2022, 12, 2)
    end_0 = start_0 + timedelta(minutes=1)

    # create mono time period
    kwargs = dict(
        deployment_id=0,
        file_ids=[0],
        channel=0,
        start_utc=start_0,
        file_end_utc=end_0,
        max_file_gap=0.1,
    )
    p = MonoTimePeriod(**kwargs)
    assert not p.has_ended

    # new annotation starting *after* period
    start_1 = end_0 + timedelta(seconds=3)
    end_1 = start_1 + timedelta(seconds=1)
    p.new_annotation(start_1, end_1)
    assert p.has_ended
    assert p.end_utc == start_1
    assert p.file_ids == [0]


def test_mono_time_period_new_file_large_gap():

    start_0 = datetime(2022, 12, 2)
    end_0 = start_0 + timedelta(minutes=1)

    # create mono time period
    kwargs = dict(
        deployment_id=0,
        file_ids=[0],
        channel=0,
        start_utc=start_0,
        file_end_utc=end_0,
        max_file_gap=0.1,
    )
    p = MonoTimePeriod(**kwargs)
    assert not p.has_ended

    # new file with gap > max_file_gap
    start_1 = end_0 + timedelta(minutes=1)
    end_1 = start_1 + timedelta(minutes=1)
    p.new_file(
        file_id=1,
        file_start_utc=start_1,
        file_end_utc=end_1,
    )
    assert p.has_ended
    assert p.end_utc == end_0
    assert p.file_ids == [0]


def test_mono_time_period_new_file_small_gap():

    start_0 = datetime(2022, 12, 2)
    end_0 = start_0 + timedelta(minutes=1)

    # create mono time period
    kwargs = dict(
        deployment_id=0,
        file_ids=[0],
        channel=0,
        start_utc=start_0,
        file_end_utc=end_0,
        max_file_gap=0.1,
    )
    p = MonoTimePeriod(**kwargs)
    assert not p.has_ended

    # new file with gap < max_file_gap
    start_1 = end_0 + timedelta(seconds=0.01)
    end_1 = start_1 + timedelta(minutes=1)
    p.new_file(
        file_id=1,
        file_start_utc=start_1,
        file_end_utc=end_1,
    )
    assert not p.has_ended
    assert p.file_ids == [0, 1]
