from .interface import TableInterface

class JobInterface(TableInterface):
    def __init__(self, backend):
        super().__init__("job", backend)

    def link_file(self, file_id: int):
        pass