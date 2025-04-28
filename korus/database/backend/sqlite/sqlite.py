import sqlite3
from korus.database.backend import DatabaseBackend, TableBackend
from .tables import create_tables
import korus.database.backend.sqlite.helpers as help
import korus.database.backend.sqlite.encode as enc


class SQLiteTableBackend(TableBackend):
    def __init__(self, conn, name):
        self.conn = conn
        self.name = name

    def add(self, row):
        help.insert_row(self.conn, self.name, enc.encode_row(self.name, row))
        self.conn.commit()

    def set(self):
        pass

    def filter(self):
        pass

    def get(self, indices=None, fields=None):
        rows = help.fetch_row(self.conn, self.name, indices, fields, as_dict=True)
        rows = [enc.decode_row(self.name, row) for row in rows]
        return [tuple(list(row.values())) for row in rows]

    def add_field(self, name, type, description, default=None):
        """ OBS: only works for TEXT/INT/REAL types with default encoding/decoding"""
        help.add_column(self.conn, self.name, name, type, enc.encode_field(default))
        self.conn.commit()


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