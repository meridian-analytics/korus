from .helpers import SQLiteTableBackend, insert_row
from .encode import encode_row


class TaxonomyBackend(SQLiteTableBackend):
    def __init__(self, conn, codec):
        super().__init__(conn, "taxonomy", codec)


    def add(self, row: dict):
        created_nodes = row.pop("created_nodes")
        removed_nodes = row.pop("removed_nodes")

        # taxonomy
        c = insert_row(self.conn, self.name, self.codec.encode(row, self.name))

        tax_id = c.lastrowid

        # precursors of created nodes
        for id, (precursor_id, is_equivalent) in created_nodes.items():
            row = {
                "id": id,
                "precursor_id": precursor_id,
                "is_equivalent": is_equivalent,
                "taxonomy_id": tax_id,
            }
            c = insert_row(self.conn, "taxonomy_created_node", encode_row(row))

        # inheritors of removed nodes
        for id, (inheritor_id, is_equivalent) in removed_nodes.items():
            row = {
                "id": id,
                "inheritor_id": inheritor_id,
                "is_equivalent": is_equivalent,
                "taxonomy_id": tax_id,
            }
            c = insert_row(self.conn, "taxonomy_removed_node", encode_row(row))

        # add 'empty' label
        c.execute(
            "INSERT INTO label VALUES (NULL, ?, ?, ?, ?, ?)",
            [tax_id, None, None, None, None],
        )

        # add labels for all the source-type pairs from the taxonomy
        for sound_source in tax.all_nodes_itr():
            for sound_type in sound_source.data["sound_types"].all_nodes_itr():
                try:
                    c.execute(
                        "INSERT INTO label VALUES (NULL, ?, ?, ?, ?, ?)",
                        [
                            tax_id,
                            sound_source.tag,
                            sound_source.identifier,
                            sound_type.tag,
                            sound_type.identifier,
                        ],
                    )
                except sqlite3.IntegrityError as err_msg:
                    if not "UNIQUE constraint failed" in str(err_msg):
                        raise sqlite3.IntegrityError(str(err_msg))
         

        self.conn.commit()

    def set(self, idx: int, row: dict):
        super().set(idx, row)

    def get(self, indices: int | list[int] = None, fields: str | list[str] = None):
        return super().get(indices, fields)
