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

    expected = """   file_id  deployment_id  storage_id filename relative_path  sample_rate  num_samples start_utc format codec end_utc channel
0        1              0           0  abc.wav                       4000        40000      None   None  None    None     [1]"""
    assert str(df) == expected
