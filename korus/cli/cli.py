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