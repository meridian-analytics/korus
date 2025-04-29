import sqlite3
from korus.database.backend import DatabaseBackend, TableBackend
from .tables import create_tables
import korus.database.backend.sqlite.helpers as help
import korus.database.backend.sqlite.encode as enc


class SQLiteTableBackend(TableBackend):
    def __init__(self, conn, name, codec):
        self.conn = conn
        self.name = name
        self.codec = codec

    def add(self, row):
        help.insert_row(self.conn, self.name, self.codec.encode(row, self.name))
        self.conn.commit()

    def set(self):
        pass

    def filter(self):
        pass

    def get(self, indices=None, fields=None):
        rows = help.fetch_row(self.conn, self.name, indices, fields, as_dict=True)
        rows = [self.codec.decode(row, self.name) for row in rows]
        return [tuple(list(row.values())) for row in rows]

    def add_field(self, name, type, description, default=None):
        help.add_column(self.conn, self.name, name, type, self.codec.encode(default))
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

        # add decoding rules
        self.codec = enc.Codec()
        self.codec.decoder.add_rule("file", "start_utc", enc.decode_datetime)

        # table backends
        self._deployment = SQLiteTableBackend(self, "deployment", self.codec)
        self._annotation = SQLiteTableBackend(self, "annotation", self.codec)
        self._file = SQLiteTableBackend(self, "file", self.codec)
        self._job = SQLiteTableBackend(self, "job", self.codec)
        self._storage = SQLiteTableBackend(self, "storage", self.codec)

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