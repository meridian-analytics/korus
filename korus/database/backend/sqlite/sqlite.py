import sqlite3
from korus.database.backend import TableBackend, DatabaseBackend
from .codec import Codec, encode_key, encode_condition
from .tables import create_tables
from .codec import create_codec
from .query import (
    get_row_count,
    insert_row,
    update_row,
    fetch_row,
    get_sqlite_type,
    where_condition,
    search_table,
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

    def __len__(self):
        return get_row_count(self.conn, self.name)

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
        condition: dict = None,
        invert: bool = False,
        indices: list[int] = None,
        **kwargs,
    ) -> list[int]:
        indices = self.codec.encode(indices, self.name, "id")
        cond = encode_condition(self.name, condition, self.codec.encode)
        cond = where_condition(self.conn, self.name, cond, invert)
        indices = search_table(self.conn, self.name, cond, indices)
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
        self._job = SQLiteTableBackend(self, "job", self.codec)
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
