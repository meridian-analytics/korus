from .helpers import SQLiteTableBackend


class StorageBackend(SQLiteTableBackend):
    def __init__(self, conn, codec):
        super().__init__(conn, "storage", codec)
