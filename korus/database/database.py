from .interface.deployment import DeploymentInterface
from .interface.annotation import AnnotationInterface
from .interface.file import FileInterface
from .interface.job import JobInterface
from .interface.storage import StorageInterface
from .backend.sqlite import SQLiteBackend


class DatabaseInterface:
    def __init__(self):
        self.deployment = DeploymentInterface()
        self.annotation = AnnotationInterface()
        self.file = FileInterface()
        self.job = JobInterface()
        self.storage = StorageInterface()

    def __str__(self):
        """Prints a pretty schematic of the database interface"""
        pass


class SQLiteDatabase(SQLiteBackend, DatabaseInterface):
    
    def __init__(self):
        super().__init__()

