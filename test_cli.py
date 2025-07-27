import korus.cli.cli as cli
from korus.database import SQLiteDatabase


db = SQLiteDatabase("test.sqlite")

#cli.cli_fcn(db)


cli.add_row(db, "deployment")