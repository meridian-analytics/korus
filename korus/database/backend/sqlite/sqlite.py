import sqlite3
from korus.database.backend import TableBackend, DatabaseBackend
from .codec import Codec, encode_key, encode_condition, decode_key
from .tables import create_tables
from .codec import create_codec
from .query import (
    get_row_count,
    insert_row,
    update_row,
    fetch_row,
    get_sqlite_type,
    where_condition,
    query_table,
    add_column,
)


class SQLiteTableBackend(TableBackend):
    """Generic SQLite table backend.

    Args:
        conn: sqlite3.Connection
            The database connection
        name: str
            The table name
        codec: korus.database.backend.sqlite.encode.Codec
            Encoder-decoder for inserting and retrieving data from the database
    """

    def __init__(self, conn: sqlite3.Connection, name: str, codec: Codec):
        super().__init__(name)
        self.conn = conn
        self.codec = codec
        self.reset_cursor()

    def __len__(self):
        return get_row_count(self.conn, self.name)

    def __next__(self):
        return decode_key(next(self._cursor)[0])

    def reset_cursor(self):
        self._cursor = self.conn.cursor().execute(f"SELECT id FROM {self.name}")

    def add(self, row: dict):
        insert_row(self.conn, self.name, self.codec.encode(row, self.name))
        self.conn.commit()

    def set(self, idx: int, row: dict):
        update_row(
            self.conn, self.name, encode_key(idx), self.codec.encode(row, self.name)
        )
        self.conn.commit()

    def get(
        self,
        indices: int | list[int] = None,
        fields: str | list[str] = None,
        return_indices: bool = False,
    ) -> list[tuple]:
        rows = fetch_row(
            self.conn,
            self.name,
            encode_key(indices),
            fields,
            as_dict=True,
            return_indices=return_indices,
        )
        rows = [self.codec.decode(row, self.name) for row in rows]
        return [tuple(list(row.values())) for row in rows]

    def filter(
        self,
        *conditions: dict,
        indices: list[int] = None,
        **kwargs,
    ) -> list[int]:
        indices = self.codec.encode(indices, self.name, "id")
        conditions = [
            encode_condition(self.name, c, self.codec.encode) for c in conditions
        ]
        condition = where_condition(self.conn, self.name, conditions)
        indices = query_table(self.conn, self.name, condition, indices)
        indices = self.codec.decode(indices, self.name, "id")
        return indices

    def add_field(
        self,
        name: str,
        type: "typing.Any",
        default: "typing.Any" = None,
        required: bool = True,
    ):
        sqlite_type = get_sqlite_type(type)
        sqlite_default = self.codec.encode(default, self.name, name)

        add_column(
            self.conn,
            self.name,
            name,
            sqlite_type,
            required,
            sqlite_default,
        )

        self.conn.commit()


class SQLiteJobBackend(SQLiteTableBackend):
    def __init__(self, conn: sqlite3.Connection, codec: Codec):
        super().__init__(conn, "job", codec)

    def add_file(self, job_id: int, file_id: int, channel: int = 0):
        tbl_name = "file_job_relation"
        row = {"job_id": job_id, "file_id": file_id, "channel": channel}
        insert_row(self.conn, tbl_name, self.codec.encode(row, tbl_name))
        self.conn.commit()

    def get_files(self, job_id: int | list[int]) -> list[tuple[int, int]]:
        tbl_name = "file_job_relation"

        # query condition
        cond = {"job_id": job_id}
        cond = encode_condition(tbl_name, cond, self.codec.encode)
        cond = where_condition(self.conn, tbl_name, cond)

        # perform query
        indices = query_table(self.conn, tbl_name, cond)

        # retrieve row data
        rows = fetch_row(
            self.conn, tbl_name, indices, fields=["file_id", "channel"], as_dict=True
        )
        rows = [self.codec.decode(row, self.name) for row in rows]
        rows = [tuple(list(row.values())) for row in rows]

        # unique, sorted list
        rows = sorted(list(set(rows)))

        return rows


class SQLiteBackend(DatabaseBackend, sqlite3.Connection):
    def __init__(self, *args, **kwargs):

        # initialize parent class
        super().__init__(*args, **kwargs)

        # enable foreign keys
        self.execute("PRAGMA foreign_keys = on")

        # create SQLite tables, if they don't already exist
        create_tables(self)

        # commit changes to SQLite database
        self.commit()

        # add field-specific encoding and decoding rules
        self.codec = create_codec(self)

        # table backends
        self._annotation = SQLiteTableBackend(self, "annotation", self.codec)
        self._deployment = SQLiteTableBackend(self, "deployment", self.codec)
        self._file = SQLiteTableBackend(self, "file", self.codec)
        self._job = SQLiteJobBackend(self, self.codec)
        self._storage = SQLiteTableBackend(self, "storage", self.codec)
        self._taxonomy = SQLiteTableBackend(self, "taxonomy", self.codec)
        self._label = SQLiteTableBackend(self, "label", self.codec)
        self._tag = SQLiteTableBackend(self, "tag", self.codec)
        self._granularity = SQLiteTableBackend(self, "granularity", self.codec)

    @property
    def deployment(self) -> SQLiteTableBackend:
        return self._deployment

    @property
    def annotation(self) -> SQLiteTableBackend:
        return self._annotation

    @property
    def file(self) -> SQLiteTableBackend:
        return self._file

    @property
    def job(self) -> SQLiteTableBackend:
        return self._job

    @property
    def storage(self) -> SQLiteTableBackend:
        return self._storage

    @property
    def taxonomy(self) -> SQLiteTableBackend:
        return self._taxonomy

    @property
    def label(self) -> SQLiteTableBackend:
        return self._label

    @property
    def tag(self) -> SQLiteTableBackend:
        return self._tag

    @property
    def granularity(self) -> SQLiteTableBackend:
        return self._granularity
