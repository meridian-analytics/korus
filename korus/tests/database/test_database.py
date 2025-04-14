import os
from korus.database import SQLiteDatabase


file_dir = os.path.dirname(os.path.realpath(__file__))
path_to_assets = os.path.join(file_dir, "..", "assets")
path_to_tmp = os.path.join(path_to_assets, "tmp")


def test_create_database():
    path = os.path.join(path_to_tmp, "test.sqlite")
    if os.path.exists(path):
        os.remove(path)

    db = SQLiteDatabase(path)

    print(db.file)

    row = dict(
        deployment_id = 1,
        storage_id = 2,
        filename = "xyz.wav",
        relative_path = "a/b/c",
        sample_rate = 96000,        
    )

    db.file.add(row)

