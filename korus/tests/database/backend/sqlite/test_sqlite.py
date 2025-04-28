import os
from datetime import datetime, timezone
from korus.database.backend.sqlite import SQLiteBackend


file_dir = os.path.dirname(os.path.realpath(__file__))
path_to_assets = os.path.join(file_dir, "..", "..", "..", "assets")
path_to_tmp = os.path.join(path_to_assets, "tmp")


def test_sqlite_backend():
    path = os.path.join(path_to_tmp, "test.sqlite")
    if os.path.exists(path):
        os.remove(path)

    db = SQLiteBackend(path)

    # insert two rows of data into the file table
    row1 = dict(
        deployment_id = 1,
        storage_id = 2,
        filename = "xyz.wav",
        relative_path = "a/b/c",
        sample_rate = 96000,
        num_samples = 960000,
        start_utc = datetime(2022, 12, 2, 2, 0, 0, 123456, tzinfo=timezone.utc)
    )

    row2 = dict(
        deployment_id = 2,
        storage_id = 3,
        filename = "ZYX.FLAC",
        relative_path = "h\i\j",
        sample_rate = 4000,
        num_samples = 40000,
        start_utc = datetime(2024, 1, 24, 6, 0, 0)
    )

    db.file.add(row1)
    db.file.add(row2)

    # retrieve the data again
    rows = db.get()
    print(rows)

    rows = db.get(fields="sample_rate")
    print(rows)

    rows = db.get(fields=["sample_rate", "start_utc"])
    print(rows)

    rows = db.get(indices=1, fields="sample_rate")
    print(rows)

    rows = db.get(indices=[2, 1], fields="sample_rate")
    print(rows)

    # close connection to database
    db.close()
