import sqlite3
from korus.database.backend import DatabaseBackend
from .deployment import DeploymentBackend
from .annotation import AnnotationBackend
from .file import FileBackend
from .job import JobBackend
from .storage import StorageBackend
from .tables import create_tables
from korus.database.backend.sqlite.helpers import SQLiteTableBackend
import korus.database.backend.sqlite.encode as enc


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
        self._deployment = DeploymentBackend(self, self.codec)
        self._annotation = AnnotationBackend(self, self.codec)
        self._file = FileBackend(self, self.codec)
        self._job = JobBackend(self, self.codec)
        self._storage = StorageBackend(self, self.codec)

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