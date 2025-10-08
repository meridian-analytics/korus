import os
import sqlite3
from datetime import datetime
from korus.database.backend import TableBackend, DatabaseBackend
from .codec import (
    Codec,
    encode_condition,
    decode_key,
    index_to_key,
    key_to_index,
    decode_row,
    decode_str_by_type,
)
from .tables import create_tables, field_table_name, table_exists, create_field_table
from .codec import create_codec, decode_bool, decode_datetime, decode_json
from .query import (
    get_row_count,
    insert_row,
    update_row,
    fetch_row,
    get_sqlite_type,
    where_condition,
    query_table,
    add_column,
    delete_row,
)


def rename_key(x, old_name, new_name):
    """Helper function for renaming key in dict"""
    x[new_name] = x.pop(old_name)
    return x


class SQLiteTableBackend(TableBackend):
    """Generic SQLite table backend.

    Note: The SQLite table and its associated _field table must be created before instantiating this class.

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

        # add codecs for saved fields
        for field in self.get_fields():
            self.add_codec(field["name"], field["type"])

    def __len__(self):
        return get_row_count(self.conn, self.name)

    def __next__(self):
        return decode_key(next(self._cursor)[0])

    def reset_cursor(self):
        self._cursor = self.conn.cursor().execute(f"SELECT id FROM {self.name}")

    def add(self, row: dict) -> int:
        cursor = insert_row(self.conn, self.name, self.codec.encode(row, self.name))
        self.conn.commit()
        return decode_key(cursor.lastrowid)

    def remove(self, indices: int | list[int] = None):
        delete_row(self.conn, self.name, index_to_key(indices))
        self.conn.commit()

    def update(self, idx: int, row: dict):
        update_row(
            self.conn, self.name, index_to_key(idx), self.codec.encode(row, self.name)
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
            index_to_key(indices),
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
        indices = index_to_key(indices)
        conditions = [
            encode_condition(self.name, c, self.codec.encode) for c in conditions
        ]
        condition = where_condition(self.conn, self.name, conditions)
        indices = query_table(self.conn, self.name, condition, indices)
        indices = key_to_index(indices)
        return indices

    def add_field(
        self,
        name: str,
        type: "typing.Any",
        default: "typing.Any" = None,
        required: bool = True,
    ):
        # add encoding/decoding rules
        self.add_codec(name, type)

        # encode type and default value
        sqlite_type = get_sqlite_type(type)
        sqlite_default = self.codec.encode(default, self.name, name)

        # add column to SQLite table
        add_column(
            self.conn,
            self.name,
            name,
            sqlite_type,
            required,
            sqlite_default,
        )

        self.conn.commit()

    def add_codec(self, field_name: str, field_type: "typing.Any"):
        """Add default encoding and decoding rules for the specified field.

        Args:
            field_name: str
                The field's name
            field_type:
                The field's type
        """
        if field_type in [tuple, list, dict]:
            self.codec.decoder.add_rule(self.name, field_name, decode_json)
        elif field_type == bool:
            self.codec.decoder.add_rule(self.name, field_name, decode_bool)
        elif field_type == datetime:
            self.codec.decoder.add_rule(self.name, field_name, decode_datetime)

    def save_field(self, field_attrs: dict):
        tbl_name = field_table_name(self.name)

        # if _field table does not exist yet, create it
        if not table_exists(self.conn, tbl_name):
            create_field_table(self.conn, self.name)

        # rename: default -> default_value
        row = field_attrs.copy()
        row["default_value"] = row.pop("default", None)

        # unsupported special case:
        if row["type"] == datetime and row.get("options", None) is not None:
            err_msg = f"Saving of custom `datetime` fields with restricted range of allowed values (`options`) is currently not supported"
            raise NotImplementedError(err_msg)

        # add field metadata to _field table
        insert_row(self.conn, tbl_name, self.codec.encode(row, tbl_name))
        self.conn.commit()

        # add column to primary table
        self.add_field(
            name=row["name"],
            type=row["type"],
            default=row["default_value"],
            required=row.get("required", True),
        )

    def get_fields(self) -> list[dict]:
        tbl_name = field_table_name(self.name)

        # if _field table does not exist, return an empty list
        if not table_exists(self.conn, tbl_name):
            return []

        # fetch data
        rows = fetch_row(self.conn, tbl_name, as_dict=True)

        # apply decoding rules
        rows = [self.codec.decode(row, tbl_name) for row in rows]

        # 'manually' decode default value
        rows = [
            decode_row(
                row,
                fcns={"default_value": lambda x: decode_str_by_type(x, row["type"])},
            )
            for row in rows
        ]

        # rename: default_value -> default
        rows = [rename_key(row, "default_value", "default") for row in rows]

        return rows


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
    def __init__(self, path: str, new: bool = False, **kwargs):

        if not new and not os.path.exists(path):
            err_msg = f"SQLite database {path} does not exist. To create a new database set new=True."
            raise OSError(err_msg)

        # initialize parent class
        super().__init__(path, **kwargs)

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
