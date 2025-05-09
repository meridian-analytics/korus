from .helpers import SQLiteTableBackend


class LabelBackend(SQLiteTableBackend):
    def __init__(self, conn, codec):
        super().__init__(conn, "label", codec)
