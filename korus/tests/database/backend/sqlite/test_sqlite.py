import os
from datetime import datetime, timezone
from korus.database.backend.sqlite import SQLiteBackend


file_dir = os.path.dirname(os.path.realpath(__file__))
path_to_assets = os.path.join(file_dir, "..", "..", "..", "assets")
path_to_tmp = os.path.join(path_to_assets, "tmp")


def test_sqlite_backend_file(minimal_sqlite_backend):
    """Test add/get/set/filter methods for the File backend"""
    db = minimal_sqlite_backend

    # insert two rows of data into the file table
    # (note that the fixture already has 1 row of data)
    row1 = dict(
        deployment_id=0,
        storage_id=0,
        filename="xyz.wav",
        relative_path="a/b/c",
        sample_rate=96000,
        num_samples=960000,
        start_utc=datetime(2022, 12, 2, 2, 0, 0, 123456, tzinfo=timezone.utc),
    )

    row2 = dict(
        deployment_id=0,
        storage_id=0,
        filename="ZYX.FLAC",
        relative_path="h\\i\\j",
        sample_rate=4000,
        num_samples=40000,
        start_utc=datetime(2024, 1, 24, 6, 0, 0, tzinfo=timezone.utc),
    )

    db.file.add(row1)
    db.file.add(row2)

    # retrieve the data again
    rows = db.file.get()

    # there should be 3 rows (because the database came pre-populated with a single row)
    assert len(rows) == 3

    # we can retrieve data for a single field
    rows = db.file.get(fields="sample_rate")
    assert len(rows) == 3
    assert len(rows[0]) == 1
    assert rows[1][0] == row1["sample_rate"]
    assert rows[2][0] == row2["sample_rate"]

    # we can retrieve data for multiple fields
    rows = db.file.get(fields=["sample_rate", "start_utc"])
    assert len(rows[0]) == 2
    assert rows[1][1] == row1["start_utc"]
    assert rows[2][1] == row2["start_utc"]

    # we can select a single row
    rows = db.file.get(indices=2, fields=["id", "sample_rate"])
    assert len(rows) == 1
    assert rows[0][0] == 2
    assert rows[0][1] == row2["sample_rate"]

    # row ordering is preserved when fetching multiple rows
    rows = db.file.get(indices=[2, 0, 1], fields=["id", "sample_rate"])
    assert len(rows) == 3
    assert rows[0][0] == 2
    assert rows[0][1] == row2["sample_rate"]
    assert rows[1][0] == 0
    assert rows[2][0] == 1

    # we can update a single row
    rows = db.file.set(idx=2, row={"sample_rate": 8000})
    rows = db.file.get(indices=2, fields="sample_rate")
    assert rows[0][0] == 8000

    # filter 
    cond = {"filename": "ZYX.FLAC"}
    indices = db.file.filter(cond)
    assert indices == [2]

    cond = {"filename": ["ZYX.FLAC"]}
    indices = db.file.filter(cond)
    assert indices == [2]

    cond = {"filename": ["xyz.wav", "ZYX.FLAC"]}
    indices = db.file.filter(cond)
    assert sorted(indices) == [1, 2]
    indices = db.file.filter(cond, indices=[0,1])
    assert indices == [1]
    indices = db.file.filter(cond, invert=True)
    assert indices == [0]

    cond = {"sample_rate": (9000,None)}
    indices = db.file.filter(cond)
    assert sorted(indices) == [0, 1]
    indices = db.file.filter(cond, invert=True)
    assert indices == [2]


def test_sqlite_backend_taxonomy(minimal_sqlite_backend):
    """Test add/get/set methods for the Taxonomy backend"""
    db = minimal_sqlite_backend

    #
