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


def test_get_file_data(job_interface_with_data):
    """Check that the get_file_data method works as it should"""

    job = job_interface_with_data

    # get file data
    df = job.get_file_data(0)

    expected = """  channel codec  deployment_id end_utc  file_id filename format  num_samples relative_path  sample_rate start_utc  storage_id
0     [1]  None              0    None        1  abc.wav   None        40000                       4000      None           0"""
    assert str(df[sorted(df.columns)]) == expected
