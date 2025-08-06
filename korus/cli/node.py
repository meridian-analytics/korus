from korus.database.database import Database
import korus.cli.prompt as prompt
import korus.cli.add as add
import korus.cli.update as upd
import korus.cli.view as vw


class Node:
    def __init__(self, id, name, fcn, args = None):
        self.id = id
        self.name = name
        self.fcn = fcn
        self.args = () if args is None else args

    def __call__(self):
        try:
            id = self.fcn(*self.args)

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
        fcn = prompt.select_table,
        args = (db,)
    )

    # table action selection nodes
    for table_name in db.tables.keys():
        id = node_id(table_name)
        nodes[id] = Node(
            id = id,
            name = table_name,
            fcn = prompt.select_table_action,
            args = (table_name,)
        )

    # table action execution nodes
    for table_name in db.tables.keys():
        # info
        id = node_id(table_name, "info")
        nodes[id] = Node(
            id = id,
            name = "info",
            fcn = vw.view_info,
            args = (db, table_name)
        )

        # contents-condensed
        id = node_id(table_name, "contents-condensed")
        nodes[id] = Node(
            id = id,
            name = "contents-condensed",
            fcn = vw.view_contents_condensed,
            args = (db, table_name)
        )

        # contents-detailed
        id = node_id(table_name, "contents-detailed")
        nodes[id] = Node(
            id = id,
            name = "contents-detailed",
            fcn = vw.view_contents_detailed,
            args = (db, table_name)
        )

        # add
        id = node_id(table_name, "add")
        nodes[id] = Node(
            id = id,
            name = "add",
            fcn = add.add,
            args = (db, table_name)
        )

        # update
        id = node_id(table_name, "update")
        nodes[id] = Node(
            id = id,
            name = "update",
            fcn = upd.update,
            args = (db, table_name)
        )

    # field action selection nodes

    return nodes, main_id