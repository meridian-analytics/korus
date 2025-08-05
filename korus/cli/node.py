from korus.database.database import Database
import korus.cli.prompt as prompt
import korus.cli.add as add
import korus.cli.view as vw


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

    # table selection node (main/root node)
    nodes[main_id] = Node(
        id = main_id,
        name = "main", 
        fcn = lambda: prompt.select_table(db)
    )

    # table action selection nodes
    for table_name in db.tables.keys():
        id = node_id(table_name)
        nodes[id] = Node(
            id = id,
            name = table_name,
            fcn = lambda: prompt.select_table_action(table_name)
        )

    # table action execution nodes
    for table_name in db.tables.keys():
        # info
        id = node_id(table_name, "info")
        nodes[id] = Node(
            id = id,
            name = "info",
            fcn = lambda: vw.view_info(db, table_name)
        )

        # add
        id = node_id(table_name, "add")
        nodes[id] = Node(
            id = id,
            name = "add",
            fcn = lambda: add.add(db, table_name)
        )

    # field menus

    return nodes, main_id