from .interface import TableInterface


class FileInterface(TableInterface):
    """ Defines the interface of the File Table.
        
    """
    def __init__(self, backend):
        super().__init__("file", backend)

        self.add_field("deployment_id", int, "Deployment index")
        self.add_field("storage_id", int, "Storage index")
        self.add_field("filename", str, "Filename")
        self.add_field("relative_path", str, "Directory path")
        self.add_field("sample_rate", int, "Sampling rate in Hz")

