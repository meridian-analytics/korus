import sqlite3
import korus.database.sqlite.table as tbl
from .interface import (
    SQLiteDeploymentBackend,
    SQLiteAnnotationBackend,
    SQLiteFileBackend,
    SQLiteJobBackend,
    SQLiteStorageBackend,
)


class SQLiteBackend(sqlite3.Connection):
    def __init__(self, *args, **kwargs):

        # initialize parent class
        super().__init__(*args, **kwargs)

        # enable foreign keys
        self.execute("PRAGMA foreign_keys = on")

        # create SQLite tables, if they don't already exist
        tbl.create_tables(self)

        # commit changes to SQLite database
        self.commit()

        # create table backends
        self.deployment_backend = SQLiteDeploymentBackend()
        self.annotation_backend = SQLiteAnnotationBackend()
        self.file_backend = SQLiteFileBackend()
        self.job_backend = SQLiteJobBackend()
        self.storage_backend = SQLiteStorageBackend()
        
