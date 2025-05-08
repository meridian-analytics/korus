import os
from datetime import datetime, timezone
from korus.database.backend.sqlite import SQLiteBackend


file_dir = os.path.dirname(os.path.realpath(__file__))
path_to_assets = os.path.join(file_dir, "..", "..", "..", "assets")
path_to_tmp = os.path.join(path_to_assets, "tmp")


def test_sqlite_backend(minimal_sqlite_backend):
    # OBS: note that the fixture already has 1 row of data
    db = minimal_sqlite_backend

    # insert two rows of data into the file table
    row1 = dict(
        deployment_id=1,
        storage_id=1,
        filename="xyz.wav",
        relative_path="a/b/c",
        sample_rate=96000,
        num_samples=960000,
        start_utc=datetime(2022, 12, 2, 2, 0, 0, 123456, tzinfo=timezone.utc),
    )

    row2 = dict(
        deployment_id=1,
        storage_id=1,
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
    assert rows[0][0] == 3  #SQLite starts indexing at 1!
    assert rows[0][1] == row2["sample_rate"]

    # row ordering is preserved when fetching multiple rows
    rows = db.file.get(indices=[2, 0, 1], fields=["id", "sample_rate"])
    assert len(rows) == 3
    assert rows[0][0] == 3  
    assert rows[0][1] == row2["sample_rate"]
    assert rows[1][0] == 1
    assert rows[2][0] == 2

    # we can update a single row
    rows = db.file.set(idx=2, row={"sample_rate": 8000})
    rows = db.file.get(indices=2, fields="sample_rate")
    assert rows[0][0] == 8000
