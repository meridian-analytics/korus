import os
from datetime import datetime, timezone


file_dir = os.path.dirname(os.path.realpath(__file__))
path_to_assets = os.path.join(file_dir, "..", "..", "..", "assets")
path_to_tmp = os.path.join(path_to_assets, "tmp")


def test_add_file_to_job(minimal_sqlite_backend):
    """Test that we can add an audiofile to a job"""
    backend = minimal_sqlite_backend

    # add a 2nd job
    backend.job.add({"taxonomy_id": 0})

    # add a 2nd file
    backend.file.add(
        {
            "deployment_id": 0,
            "storage_id": 0,
            "filename": "xyz.flac",
            "sample_rate": 2000,
            "num_samples": 20000,
        }
    )

    # link 1st file to the 2nd job
    backend.job.add_file(1, 0)

    # link 2nd file to both jobs
    backend.job.add_file(0, 1)
    backend.job.add_file(1, 1)

    # verify that files were linked
    file_ids = backend.job.get_files(0)
    assert file_ids == [1]
    file_ids = backend.job.get_files(1)
    assert file_ids == [0, 1]
    file_ids = backend.job.get_files([0, 1])
    assert file_ids == [0, 1]


def test_table_backend(minimal_sqlite_backend):
    """Test add,get,set,filter methods for SQLiteTableBackend"""
    backend = minimal_sqlite_backend

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

    backend.file.add(row1)
    backend.file.add(row2)

    # retrieve the data again
    rows = backend.file.get()

    # there should be 3 rows (because the database came pre-populated with a single row)
    assert len(rows) == 3

    # we can retrieve data for a single field
    rows = backend.file.get(fields="sample_rate")
    assert len(rows) == 3
    assert len(rows[0]) == 1
    assert rows[1][0] == row1["sample_rate"]
    assert rows[2][0] == row2["sample_rate"]

    # we can retrieve data for multiple fields
    rows = backend.file.get(fields=["sample_rate", "start_utc"])
    assert len(rows[0]) == 2
    assert rows[1][1] == row1["start_utc"]
    assert rows[2][1] == row2["start_utc"]

    # we can select a single row
    rows = backend.file.get(indices=2, fields=["id", "sample_rate"])
    assert len(rows) == 1
    assert rows[0][0] == 2
    assert rows[0][1] == row2["sample_rate"]

    # row ordering is preserved when fetching multiple rows
    rows = backend.file.get(indices=[2, 0, 1], fields=["id", "sample_rate"])
    assert len(rows) == 3
    assert rows[0][0] == 2
    assert rows[0][1] == row2["sample_rate"]
    assert rows[1][0] == 0
    assert rows[2][0] == 1

    # we can update a single row
    rows = backend.file.set(idx=2, row={"sample_rate": 8000})
    rows = backend.file.get(indices=2, fields="sample_rate")
    assert rows[0][0] == 8000

    # filtering
    cond = {"filename": "ZYX.FLAC"}
    indices = backend.file.filter(cond)
    assert indices == [2]

    cond = {"filename": ["ZYX.FLAC"]}
    indices = backend.file.filter(cond)
    assert indices == [2]

    cond = {"sample_rate": 8000}
    indices = backend.file.filter(cond)
    assert indices == [2]

    cond = {"filename": ["xyz.wav", "ZYX.FLAC"]}
    indices = backend.file.filter(cond)
    assert sorted(indices) == [1, 2]
    indices = backend.file.filter(cond, indices=[0, 1])
    assert indices == [1]

    cond = {"filename~": ["xyz.wav", "ZYX.FLAC"]}
    indices = backend.file.filter(cond)
    assert indices == [0]

    cond = {"sample_rate": (9000, None)}
    indices = backend.file.filter(cond)
    assert sorted(indices) == [0, 1]

    cond = {"sample_rate~": (9000, None)}
    indices = backend.file.filter(cond)
    assert indices == [2]

    t = datetime(2022, 12, 2, 2, 0, 0, 123457, tzinfo=timezone.utc)
    cond = {"sample_rate": 100, "start_utc": (t, None)}
    indices = backend.file.filter(cond)
    assert indices == []

    cond = {"sample_rate": (100, None), "start_utc": (t, None)}
    indices = backend.file.filter(cond)
    assert indices == [2]

    t = datetime(2022, 12, 2, 2, 0, 0, 123455)
    cond = {"sample_rate": (100, None), "start_utc": (t, None)}
    indices = backend.file.filter(cond)
    assert sorted(indices) == [1, 2]

    # filtering on JSON columns
    backend.annotation.add(
        {"deployment_id": 0, "job_id": 0, "excluded_label_id": [0, 1]}
    )
    backend.annotation.add(
        {"deployment_id": 0, "job_id": 0, "excluded_label_id": [0, 2]}
    )
    cond = {"excluded_label_id": 0}
    indices = backend.annotation.filter(cond)
    assert sorted(indices) == [1, 2]

    cond = {"excluded_label_id": 2}
    indices = backend.annotation.filter(cond)
    assert indices == [2]
