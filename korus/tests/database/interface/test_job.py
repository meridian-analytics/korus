import pytest
from korus.database.interface import (
    FileInterface,
    JobInterface,
)
from korus.tests.helpers import InMemoryTableBackend, InMemoryJobBackend


def test_get_files(job_interface_with_data):
    """Check that the get_files method works as it should"""

    job = job_interface_with_data

    # get files associated with 1st job
    rows = job.get_files(0)
    assert rows == [(1, 14)]

    # get files associated with 2nd job
    rows = job.get_files(1)
    assert rows == [(0, 0), (1, 14)]


def test_get_filedata(job_interface_with_data):
    """Check that the get_file_data method works as it should"""

    job = job_interface_with_data

    # get file data
    df = job.get_filedata(0)

    expected = """  channel codec  deployment_id end_utc  file_id filename format  num_samples relative_path  sample_rate start_utc  storage_id
0    [14]  None              0     NaT        1  abc.wav   None        40000                       4000       NaT           0"""
    answer = df[sorted(df.columns)].to_string()
    assert answer == expected
    