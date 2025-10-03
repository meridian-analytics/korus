from korus.database.database import Database
from .module import create_modules
from .cursor import cursor


def main(db: Database):
    modules, id = create_modules(db)

    while id is not None:
        m = modules.get(id)
        cursor.go_to(m)
        id = cursor.execute()
