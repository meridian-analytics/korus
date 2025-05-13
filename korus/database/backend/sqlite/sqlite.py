import sqlite3
from korus.database.backend import DatabaseBackend
from .annotation import AnnotationBackend
from .tables import create_tables
from korus.database.backend.sqlite.helpers import get_table_names, SQLiteTableBackend
import korus.database.backend.sqlite.encode as enc


def create_codec(conn):
    codec = enc.Codec()

    # decode timestamps
    codec.decoder.add_rule("deployment", "start_utc", enc.decode_datetime)
    codec.decoder.add_rule("deployment", "end_utc", enc.decode_datetime)
    codec.decoder.add_rule("file", "start_utc", enc.decode_datetime)

    # millisecod fields in annotation table
    codec.encoder.add_rule("annotation", "duration_ms", enc.encode_ms)
    codec.decoder.add_rule("annotation", "duration_ms", enc.decode_ms)
    codec.encoder.add_rule("annotation", "start_ms", enc.encode_ms)
    codec.decoder.add_rule("annotation", "start_ms", enc.decode_ms)

    # file_id_list field in annotation table
    codec.encoder.add_rule("annotation", "file_id_list", enc.encode_key)
    codec.decoder.add_rule("annotation", "file_id_list", enc.decode_key)

    # encode & decode table keys
    table_names = get_table_names(conn)
    for table_name in table_names:
        # primary keys
        codec.encoder.add_rule(table_name, "id", enc.encode_key)
        codec.decoder.add_rule(table_name, "id", enc.decode_key)

        for key_name in table_names:
            # foreign keys
            key_name += "_id"
            codec.encoder.add_rule(table_name, key_name, enc.encode_key)
            codec.decoder.add_rule(table_name, key_name, enc.decode_key)

    return codec


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
        self._annotation = AnnotationBackend(self, self.codec)
        self._deployment = SQLiteTableBackend(self, "deployment", self.codec)
        self._file = SQLiteTableBackend(self, "file", self.codec)
        self._job = SQLiteTableBackend(self, "job", self.codec)
        self._storage = SQLiteTableBackend(self, "storage", self.codec)
        self._taxonomy = SQLiteTableBackend(self, "taxonomy", self.codec)
        self._label = SQLiteTableBackend(self, "label", self.codec)

    @property
    def deployment(self) -> SQLiteTableBackend:
        return self._deployment

    @property
    def annotation(self) -> AnnotationBackend:
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

    def add_tag(self, name: str, description: str):
        """Add an annotation tag.

        Args:
            name: str
                The tag name
            description: str
                A short description of the tag's intended use
        """
        help.insert_row(self, "tag", {"name": name, "description": description})
        self.commit()

    def add_granularity(self, name: str, description: str):
        """Add an annotation granularity level.

        Args:
            name: str
                The granularity level's name
            description: str
                A short definition of the granularity level
        """
        help.insert_row(self, "granularity", {"name": name, "description": description})
        self.commit()
