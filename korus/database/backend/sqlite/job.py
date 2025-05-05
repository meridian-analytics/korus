from .helpers import SQLiteTableBackend


class JobBackend(SQLiteTableBackend):
    def __init__(self, conn, codec):
        super().__init__(conn, "job", codec)
