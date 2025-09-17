import os
import korus.cli.cli as cli
from korus.database import SQLiteDatabase

path = "test.sqlite"


if os.path.exists(path):
    y = input("remove existing db? [y/n]")
    if y == "y":
        os.remove(path)

db = SQLiteDatabase(path)


cli.main(db)

#cli.cli_fcn(db)

#idx = cli.add_row(db, "storage")

#idx = cli.add_row(db, "job")

#print(idx)

#db.backend.close()

#cli.add_row(db, "deployment")
