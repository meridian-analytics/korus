from .interface.deployment import DeploymentInterface
from .interface.annotation import AnnotationInterface
from .interface.file import FileInterface
from .interface.job import JobInterface
from .interface.storage import StorageInterface


class DatabaseInterface:
    @property
    def deployment(self) -> DeploymentInterface:
        pass

    @property
    def annotation(self) -> AnnotationInterface:
        pass

    @property
    def file(self) -> FileInterface:
        pass

    @property
    def job(self) -> JobInterface:
        pass

    @property
    def storage(self) -> StorageInterface:
        pass

    def __str__(self):
        """Prints a pretty summary of the database structure"""
        pass