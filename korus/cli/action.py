import inquirer
from korus.database.database import Database
from korus.database.interface import TableViewer
import korus.cli.prompt as prompt
import korus.cli.parse as parse


def view_contents(db: Database, table_name: str):
    tbl = getattr(db, table_name)
    viewer = TableViewer(tbl)
    counter = 0
    for page in iter(viewer):
        if counter >= 1:
            proceed = inquirer.confirm("Continue?", default=True)
            if not proceed:
                break

        print(page)
        counter += 1


def add_row(db: Database, table_name: str) -> int:

    tbl = getattr(db, table_name)

    row = {}

    for field in tbl.fields:

        while True:
            action, kwargs = prompt.field_action(db, table_name, field)

            try:
                if action == prompt.FIELD_INFO:
                    print(field.info())

                if action == prompt.FIELD_ENTER:
                    value = prompt.enter_value(table_name, field, **kwargs)
                    break

                elif action == prompt.FIELD_SELECT:
                    value = prompt.select_value(table_name, field, **kwargs)
                    break

                elif action == prompt.FIELD_EXTERNAL:
                    view_contents(db, **kwargs)

                elif action == prompt.FIELD_SKIP:
                    value = None
                    break

            except KeyboardInterrupt:
                continue

        if value is not None:
            row[field.name] = parse.parse_value(field, value)

    return tbl.add(row)