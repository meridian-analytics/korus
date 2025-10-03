from .interface import TableInterface


class StorageInterface(TableInterface):
    def __init__(self, backend):
        super().__init__("storage", backend)

        self._create_field("name", str, "Name of storage location", required=True)
        self._create_field("path", str, "Directory path", default="/", is_path=True)
        self._create_field(
            "by_date",
            bool,
            "Whether audiofiles are organized into date-stamped subfolders",
            default=False,
        )
