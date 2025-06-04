from .interface import (
    DeploymentInterface,
    AnnotationInterface,
    FileInterface,
    JobInterface,
    StorageInterface,
    TaxonomyInterface,
    LabelInterface,
    TagInterface,
    GranularityInterface,
)
from .backend import DatabaseBackend
from .backend.sqlite import SQLiteBackend


class Database:

    def __init__(self, backend: DatabaseBackend):
        self.backend = backend

        self._label = LabelInterface(self.backend.label)

        self.deployment = DeploymentInterface(self.backend.deployment)
        self.file = FileInterface(self.backend.file)
        self.job = JobInterface(self.backend.job, self.file)
        self.storage = StorageInterface(self.backend.storage)
        self.taxonomy = TaxonomyInterface(self.backend.taxonomy, self._label)
        self.tag = TagInterface(self.backend.tag)
        self.granularity = GranularityInterface(self.backend.granularity)
        self.annotation = AnnotationInterface(
            self.backend.annotation,
            taxonomy=self.taxonomy,
            job=self.job,
            tag=self.tag,
            granularity=self.granularity,
        )


class SQLiteDatabase(Database):

    def __init__(self, path: str):
        super().__init__(SQLiteBackend(path))
