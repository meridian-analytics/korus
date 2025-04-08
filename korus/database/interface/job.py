from .interface import TableInterface

class JobInterface(TableInterface):
    def __init__(self):
        super().__init__("job")

    def link_file(self, file_id: int):
        pass