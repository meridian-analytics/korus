import pytest
from korus.tests.helpers import InMemoryTableBackend, InMemoryJobBackend
from korus.database.interface import (
    StorageInterface,
    FileInterface,
)


def test_get_file_ids(two_files):
    storage = StorageInterface(InMemoryTableBackend())
    file = FileInterface(InMemoryTableBackend(), storage)

    id = file.add(two_files[0])
    assert id == 0
    id = file.add(two_files[1])
    assert id == 1

    filenames = [x["filename"] for x in two_files]
    ids = file.get_id(deployment_id=0, filename=filenames)
    assert ids == [0, 1]

    ids = file.get_id(deployment_id=0, filename=filenames + ["non-existing-file"])
    assert ids == [0, 1, None]

    ids = file.get_id(deployment_id=0, filename=["non-existing-file"] + filenames)
    assert ids == [None, 0, 1]
