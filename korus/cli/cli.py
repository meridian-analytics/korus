from korus.database.database import Database
import korus.cli.prompt as prompt
import korus.cli.view as vw
from .add import add
from .update import update
from .cursor import cursor
from .node import create_nodes


def main(db: Database):
    nodes, id = create_nodes(db)

    while id is not None:
        node = nodes.get(id)
        cursor.to(node)

        id = node()

        if id is None:
            cursor.back()
            id = cursor.id
            cursor.back()


def main2(db: Database):

    while True:
        try:
            table_name = prompt.select_table(db)

        except KeyboardInterrupt:
            break

        while True:
            try:
                tbl_action = prompt.table_action(table_name)
            except KeyboardInterrupt:
                break

            try:
                if tbl_action == prompt.TABLE_ADD:
                    add(db, table_name)

                elif tbl_action == prompt.TABLE_UPDATE:
                    update(db, table_name)

                elif tbl_action == prompt.TABLE_INFO:
                    vw.view_info(db, table_name)

                elif tbl_action == prompt.TABLE_CONTENTS_CONDENSED:
                    vw.view_contents_condensed(db, table_name)

                elif tbl_action == prompt.TABLE_CONTENTS_DETAILED:
                    vw.view_contents_detailed(db, table_name)

            except KeyboardInterrupt:
                continue

    db.backend.close()
