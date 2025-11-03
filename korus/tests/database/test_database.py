import os
import pytest
from datetime import datetime
from korus.database import SQLiteDatabase


file_dir = os.path.dirname(os.path.realpath(__file__))
path_to_assets = os.path.join(file_dir, "..", "assets")
path_to_tmp = os.path.join(path_to_assets, "tmp")


def test_create_database():
    path = os.path.join(path_to_tmp, "test.sqlite")
    if os.path.exists(path):
        os.remove(path)

    db = SQLiteDatabase(path, new=True)

    row = dict(
        deployment_id=1,
        storage_id=2,
        filename="xyz.wav",
        relative_path="a/b/c",
        sample_rate=96000,
    )

    with pytest.raises(AssertionError):
        db.file.add(row)


def test_save_custom_field():
    path = os.path.join(path_to_tmp, "test.sqlite")
    if os.path.exists(path):
        os.remove(path)

    # create new database, add a custom field, then close the database
    db = SQLiteDatabase(path, new=True)
    assert "custom_field" not in db.storage.field_names
    db.storage.add_field("custom_field", int, "A custom int field", default=13)
    db.backend.close()

    # re-open the database and check that the custom field is still there
    db = SQLiteDatabase(path)
    assert "custom_field" in db.storage.field_names
    db.backend.close()
