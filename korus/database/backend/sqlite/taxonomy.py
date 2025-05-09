from .helpers import SQLiteTableBackend


class TaxonomyBackend(SQLiteTableBackend):
    def __init__(self, conn, codec):
        super().__init__(conn, "taxonomy", codec)
