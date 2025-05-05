from .interface import (
    DeploymentInterface,
    AnnotationInterface,
    FileInterface,
    JobInterface,
    StorageInterface,
    TaxonomyInterface,
)
from .backend import DatabaseBackend
from .backend.sqlite import SQLiteBackend


class Database:

    def __init__(self, backend: DatabaseBackend):
        self.backend = backend

        self.deployment = DeploymentInterface(self.backend.deployment)
        self.annotation = AnnotationInterface(self.backend.annotation)
        self.file = FileInterface(self.backend.file)
        self.job = JobInterface(self.backend.job)
        self.storage = StorageInterface(self.backend.storage)
        self.taxonomy = TaxonomyInterface(self.backend.taxonomy)


class SQLiteDatabase(Database):

    def __init__(self, path: str):
        super().__init__(SQLiteBackend(path))
