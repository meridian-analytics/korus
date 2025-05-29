from .interface import TableInterface


class TagInterface(TableInterface):
    def __init__(self, backend):
        super().__init__("tag", backend)

        self.add_field("name", str, "Tag name")
        self.add_field("description", str, "A brief description of the tag")
