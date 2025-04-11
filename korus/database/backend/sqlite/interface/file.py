import sqlite3
from dataclasses import asdict
from korus.database.interface import FileRow
from korus.database.sqlite.helpers import insert_row

class SQLiteFileBackend:
    def __init__(self, conn: sqlite3.Connection):
        super().__init__()
        self.conn = conn

    def add(self, row: FileRow, **kwargs) -> int:
        insert_row(self.conn, "file", asdict(row))