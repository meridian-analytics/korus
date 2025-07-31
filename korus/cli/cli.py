from korus.database.database import Database
import korus.cli.prompt as prompt
import korus.cli.text as txt
import korus.cli.view as vw
import korus.cli.add as add


def main(db: Database):

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
                    add.add_row(db, table_name)
                    print(txt.info(f"Successfully added row to {table_name} table."))

                elif tbl_action == prompt.TABLE_INFO:
                    tbl = getattr(db, table_name)
                    print(tbl)

                elif tbl_action == prompt.TABLE_CONTENTS_CONDENSED:
                    vw.view_contents_condensed(db, table_name)

                elif tbl_action == prompt.TABLE_CONTENTS_DETAILED:
                    vw.view_contents_detailed(db, table_name)

            except KeyboardInterrupt:
                continue

    db.backend.close()
