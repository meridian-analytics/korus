from korus.database.database import Database
import korus.cli.prompt as prompt
from .cursor import cursor


class Node:
    def __init__(self, name, id, fcn):
        self.name = name
        self.id = id
        self.fcn = fcn

    def __call__(self):
        cursor.forward(self.id)
        try:
            id = self.fcn()

        except KeyboardInterrupt:
            id = None

        if id is None:
            id = cursor.previous
            if id is not None:
                cursor.back().back()

        return id
    

def node_id(*keys):
    return ".".join(keys)


def create_nodes(db: Database) -> dict[str, Node]:

    nodes = {}
    main_id = node_id("main")

    # main menu
    nodes[main_id] = Node(
        name = "main", 
        id = main_id,
        fcn = lambda: prompt.select_table(db)
    )

    # table menus
    for table_name in db.tables.keys():
        id = node_id(table_name)
        nodes[id] = Node(
            name = table_name,
            id = id,
            fcn = lambda: prompt.table_action(table_name)
        )

    # table actions

    # field menus

    return nodes, main_id