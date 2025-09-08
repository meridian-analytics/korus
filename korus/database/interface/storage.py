from .interface import TableInterface


class StorageInterface(TableInterface):
    def __init__(self, backend):
        super().__init__("storage", backend)

        self._create_field("name", str, "Name of storage location")
        self._create_field("path", str, "Directory path", default="/", is_path=True)
        self._create_field(
            "address", str, "URL address or physical location", required=False
        )
        self._create_field("description", str, "Brief description", required=False)
