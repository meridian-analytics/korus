import os
from korus.database import SQLiteDatabase

path = "test.sqlite"


if os.path.exists(path):
    y = input("remove existing db? [y/n]")
    if y == "y":
        os.remove(path)


db = SQLiteDatabase(path)

ti = db.taxonomy

ti.draft.create_sound_source("A")
ti.draft.create_sound_source("AA", parent="A")
ti.draft.create_sound_type("a", "A")
ti.draft.create_sound_type("aa", "A", parent="a")
ti.draft.create_sound_type("b", "A")

# make a release with no comment
ti.release()
