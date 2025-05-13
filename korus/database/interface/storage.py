from .interface import TableInterface


class StorageInterface(TableInterface):
    def __init__(self, backend):
        super().__init__("storage", backend)

        self.add_field("name", str, "Name of storage location")
        self.add_field("path", str, "Directory path", default="/")
        self.add_field(
            "address", str, "URL address or physical location", required=False
        )
        self.add_field("description", str, "Brief description", required=False)
