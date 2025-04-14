from .interface import TableInterface

class StorageInterface(TableInterface):
    def __init__(self, backend):
        super().__init__("storage", backend)