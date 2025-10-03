from korus.database.database import Database
import korus.cli.prompt.prompt as prompt
import korus.cli.prompt.add as add
import korus.cli.prompt.update as upd
import korus.cli.prompt.view as vw


class Module:
    """ """

    def __init__(self, id, name, fcn, args=None, **kwargs):
        self.id = id
        self.name = name
        self.fcn = fcn
        self.args = () if args is None else args
        self.kwargs = {} if kwargs is None else kwargs

    def __call__(self):
        try:
            return self.fcn(*self.args, **self.kwargs)
        except KeyboardInterrupt:
            return None


class SelectTable(Module):
    def __init__(self, db):
        super().__init__(
            name="main", id=module_id("main"), fcn=prompt.select_table, args=(db,)
        )


class SelectTableAction(Module):
    def __init__(self, table_name):
        super().__init__(
            name=table_name,
            id=module_id(table_name),
            fcn=prompt.select_table_action,
            args=(table_name,),
        )
        self.action_names = {
            prompt.TABLE_INFO: "info",
            prompt.TABLE_CONTENTS: "contents",
            prompt.TABLE_ADD: "add",
            prompt.TABLE_UPDATE: "update",
            prompt.TABLE_VIEW_TAXONOMY: "view_taxonomy",
        }

    def __call__(self):
        action_enum = super().__call__()

        if action_enum is None:
            return None

        else:
            action_name = self.action_names[action_enum]
            id = module_id(self.name, action_name)
            return id


class TableInfo(Module):
    def __init__(self, db, table_name):
        super().__init__(
            name="info",
            id=module_id(table_name, "info"),
            fcn=vw.view_info,
            args=(db, table_name),
        )


class TableContents(Module):
    def __init__(self, db, table_name):
        super().__init__(
            name="contents",
            id=module_id(table_name, "contents"),
            fcn=vw.view_contents,
            args=(db, table_name),
        )


class TableAdd(Module):
    def __init__(self, db, table_name):
        super().__init__(
            name="add",
            id=module_id(table_name, "add"),
            fcn=add.add,
            args=(db, table_name),
        )


class TableUpdate(Module):
    def __init__(self, db, table_name):
        super().__init__(
            name="update",
            id=module_id(table_name, "update"),
            fcn=upd.update,
            args=(db, table_name),
        )


class TableViewTaxonomy(Module):
    def __init__(self, db):
        super().__init__(
            name="view_taxonomy",
            id=module_id("taxonomy", "view_taxonomy"),
            fcn=vw.view_taxonomy,
            args=(db,),
        )


def module_id(*keys):
    return ".".join(keys)


def create_modules(db: Database) -> dict[str, Module]:

    modules = {}

    # module for selecting a table
    m = SelectTable(db)
    modules[m.id] = m

    # ID of main/root module
    main_id = m.id

    # modules for selecting table actions
    for table_name in db.tables.keys():
        m = SelectTableAction(table_name)
        modules[m.id] = m

    # modules for executing table actions
    for table_name in db.tables.keys():

        # view info
        m = TableInfo(db, table_name)
        modules[m.id] = m

        # view contents
        m = TableContents(db, table_name)
        modules[m.id] = m

        # add data
        m = TableAdd(db, table_name)
        modules[m.id] = m

        # update entries
        m = TableUpdate(db, table_name)
        modules[m.id] = m

        # view taxonomy
        m = TableViewTaxonomy(db)
        modules[m.id] = m

    return modules, main_id
