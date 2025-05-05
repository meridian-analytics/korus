from .helpers import SQLiteTableBackend


class DeploymentBackend(SQLiteTableBackend):
    def __init__(self, conn, codec):
        super().__init__(conn, "deployment", codec)
