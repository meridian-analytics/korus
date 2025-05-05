import datetime
from .interface import TableInterface


class FileInterface(TableInterface):
    """Defines the interface of the File Table."""

    def __init__(self, backend):
        super().__init__("file", backend)

        self.add_field("deployment_id", int, "Deployment index")
        self.add_field("storage_id", int, "Storage index")
        self.add_field("filename", str, "Filename")
        self.add_field("relative_path", str, "Directory path", default="")
        self.add_field("sample_rate", int, "Sampling rate in Hz")
        self.add_field("num_samples", int, "Number of samples")
        self.add_field("start_utc", datetime.datetime, "Start time of recording (UTC)")
        self.add_field("format", str, "Audio format")
        self.add_field("codec", str, "Audio codec")
