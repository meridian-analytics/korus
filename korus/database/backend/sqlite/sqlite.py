import sqlite3
from korus.database.backend import DatabaseBackend
from .table import create_tables, SQLiteTableBackend


class SQLiteBackend(DatabaseBackend, sqlite3.Connection):
    def __init__(self, *args, **kwargs):

        # initialize parent class
        super().__init__(*args, **kwargs)

        # enable foreign keys
        self.execute("PRAGMA foreign_keys = on")

        # create SQLite tables, if they don't already exist
        create_tables(self)

        # commit changes to SQLite database
        self.commit()

        # table backends
        self._deployment = SQLiteTableBackend(self, "deployment")
        self._annotation = SQLiteTableBackend(self, "annotation")
        self._file = SQLiteTableBackend(self, "file")
        self._job = SQLiteTableBackend(self, "job")
        self._storage = SQLiteTableBackend(self, "storage")

    @property
    def deployment(self) -> SQLiteTableBackend:
        return self._deployment

    @property
    def annotation(self) -> SQLiteTableBackend:
        return self._annotation
    
    @property
    def file(self) -> SQLiteTableBackend:
        return self._file
    
    @property
    def job(self) -> SQLiteTableBackend:
        return self._job
    
    @property
    def storage(self) -> SQLiteTableBackend:
        return self._storage