import sqlite3
from korus.database.backend import DatabaseBackend
from .annotation import AnnotationBackend
from .tables import create_tables
from korus.database.backend.sqlite.helpers import SQLiteTableBackend
from korus.database.backend.sqlite.encode import create_codec


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

        # add field-specific encoding and decoding rules
        self.codec = create_codec(self)

        # table backends
        self._annotation = AnnotationBackend(self, self.codec)
        self._deployment = SQLiteTableBackend(self, "deployment", self.codec)
        self._file = SQLiteTableBackend(self, "file", self.codec)
        self._job = SQLiteTableBackend(self, "job", self.codec)
        self._storage = SQLiteTableBackend(self, "storage", self.codec)
        self._taxonomy = SQLiteTableBackend(self, "taxonomy", self.codec)
        self._label = SQLiteTableBackend(self, "label", self.codec)
        self._tag = SQLiteTableBackend(self, "tag", self.codec)
        self._granularity = SQLiteTableBackend(self, "granularity", self.codec)

    @property
    def deployment(self) -> SQLiteTableBackend:
        return self._deployment

    @property
    def annotation(self) -> AnnotationBackend:
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

    @property
    def taxonomy(self) -> SQLiteTableBackend:
        return self._taxonomy

    @property
    def label(self) -> SQLiteTableBackend:
        return self._label

    @property
    def tag(self) -> SQLiteTableBackend:
        return self._tag

    @property
    def granularity(self) -> SQLiteTableBackend:
        return self._granularity
