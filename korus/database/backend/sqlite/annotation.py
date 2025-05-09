from .helpers import SQLiteTableBackend, insert_row, update_row, fetch_row, sqlite_key


class AnnotationBackend(SQLiteTableBackend):
    def __init__(self, conn, codec):
        super().__init__(conn, "annotation", codec)

    def add(self, row: dict):
        row = self._replace_labels_with_ids(row)
        super().add(row)

    def set(self, idx: int, row: dict):
        row = self._replace_labels_with_ids(row)
        super().set(idx, row)

    def get(self, indices: int | list[int] = None, fields: str | list[str] = None):       
        rows = fetch_row(
            self.conn, self.name, sqlite_key(indices), fields, as_dict=True
        )
        rows = [self.codec.decode(row, self.name) for row in rows]
        rows = [self._replace_ids_with_labels(row) for row in rows]
        return [tuple(list(row.values())) for row in rows]

    def _replace_labels_with_ids(self, row):
        sound_source = row.pop("sound_source", None)
        sound_type = row.pop("sound_type", None)
        row["label_id"] = id_from_label(self.conn, sound_source, sound_type)

        #TODO: repeat for other labels (tentative, excluded, etc.)

        return row

    def _replace_ids_with_labels(self, row):
        # TODO: implement this
        return row

def id_from_label(conn, taxonomy_id, sound_source, sound_type):
    pass
    # query `label` table for matching row
    # if found, return its id
    # otherwise, create a new entry and return its id


def label_from_id(conn, id):
    pass
    # query the `label` table