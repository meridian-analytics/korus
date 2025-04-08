import os
from korus.database.sqlite import SQLiteDatabase


file_dir = os.path.dirname(os.path.realpath(__file__))
path_to_assets = os.path.join(file_dir, "..", "..", "assets")
path_to_tmp = os.path.join(path_to_assets, "tmp")


def test_create_database():
    path = os.path.join(path_to_tmp, "test.sqlite")
    if os.path.exists(path):
        os.remove(path)

    db = SQLiteDatabase(path)

    print(db.file)

