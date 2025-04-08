import sqlite3
from dataclasses import asdict
from korus.database.interface import FileInterface, FileRow
from korus.database.sqlite.helpers import insert_row

class SQLiteFileInterface(FileInterface):
    def __init__(self, conn: sqlite3.Connection):
        super().__init__()
        self.conn = conn

    def _add_row(self, row: FileRow, **kwargs) -> int:
        insert_row(self.conn, "file", asdict(row))