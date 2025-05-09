from .helpers import SQLiteTableBackend


class AnnotationBackend(SQLiteTableBackend):
    def __init__(self, conn, codec):
        super().__init__(conn, "annotation", codec)

