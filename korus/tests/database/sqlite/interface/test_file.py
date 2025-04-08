import os
import sqlite3
from korus.database.sqlite.interface import SQLiteFileInterface


file_dir = os.path.dirname(os.path.realpath(__file__))
path_to_assets = os.path.join(file_dir, "..", "..", "..", "assets")
path_to_tmp = os.path.join(path_to_assets, "tmp")


def test_file_interface():
    path = os.path.join(path_to_tmp, "test.sqlite")
    if os.path.exists(path):
        os.remove(path)

    conn = sqlite3.connect(path)
    f = SQLiteFileInterface(conn)

    print(f)