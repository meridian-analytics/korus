from .interface import TableInterface

class StorageInterface(TableInterface):
    def __init__(self):
        super().__init__("storage")