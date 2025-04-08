import sqlite3
from korus.database.interface import DatabaseInterface
import korus.database.sqlite.table as tbl
from korus.database.sqlite.interface import (
    SQLiteDeploymentInterface,
    SQLiteAnnotationInterface,
    SQLiteFileInterface,
    SQLiteJobInterface,
    SQLiteStorageInterface,
)


class SQLiteDatabase(sqlite3.Connection, DatabaseInterface):
    def __init__(self, *args, **kwargs):
        # initialize parent classes
        super().__init__(*args, **kwargs)

        # create tables, if they don't already exist
        tbl.create_file_table(self)

        # create interfaces
        self._deployment = SQLiteDeploymentInterface(self)
        self._annotation = SQLiteAnnotationInterface(self)
        self._file = SQLiteFileInterface(self)
        self._job = SQLiteJobInterface(self)
        self._storage = SQLiteStorageInterface(self)

        # collect interfaces in a dict for easier access
        self.interfaces = {
            "deployment": self.deployment,
            "annotation": self.annotation,
            "job": self.job,
            "file": self.file,
            "storage": self.storage,
        }

    @property
    def deployment(self) -> SQLiteDeploymentInterface:
        return self._deployment

    @property
    def annotation(self) -> SQLiteAnnotationInterface:
        return self._annotation

    @property
    def file(self) -> SQLiteFileInterface:
        return self._file

    @property
    def job(self) -> SQLiteJobInterface:
        return self._job

    @property
    def storage(self) -> SQLiteStorageInterface:
        return self._storage
