from korus.database.database import Database
import korus.cli.prompt as prompt
from .cursor import cursor


class Node:
    def __init__(self, id, name, fcn):
        self.id = id
        self.name = name
        self.fcn = fcn

    def __call__(self):
        try:
            id = self.fcn()

        except KeyboardInterrupt:
            id = None

        return id
    

def node_id(*keys):
    return ".".join(keys)


def create_nodes(db: Database) -> dict[str, Node]:

    nodes = {}
    main_id = node_id("main")

    # main menu
    nodes[main_id] = Node(
        id = main_id,
        name = "main", 
        fcn = lambda: prompt.select_table(db)
    )

    # table menus
    for table_name in db.tables.keys():
        id = node_id(table_name)
        nodes[id] = Node(
            id = id,
            name = table_name,
            fcn = lambda: prompt.table_action(table_name)
        )

    # table actions

    # field menus

    return nodes, main_id