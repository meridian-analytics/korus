import sqlite3

class SQLiteStorageBackend:
    def __init__(self, conn: sqlite3.Connection):
        super().__init__()
        self.conn = conn
