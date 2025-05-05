from .helpers import SQLiteTableBackend


class FileBackend(SQLiteTableBackend):
    def __init__(self, conn, codec):
        super().__init__(conn, "file", codec)
