from .interface import TableInterface
from korus.taxonomy import AcousticTaxonomy, VersionedAcousticTaxonomy
from datetime import datetime, timezone


class TaxonomyInterface(TableInterface, VersionedAcousticTaxonomy):
    """Defines the interface of the Taxonomy Table."""

    def __init__(self, backend):
        super().__init__(name="taxonomy", backend=backend)

        self.add_field("version", int, "Version", required=False)
        self.add_field("timestamp", datetime, "Time of release")
        self.add_field("changes", list, "Changes since previous version")
        self.add_field("comment", str, "Any additional release notes", required=False)
        self.add_field("tree", dict, "Taxonomy tree")
        self.add_field(
            "created_nodes", dict, "Nodes created since the previous version"
        )
        self.add_field(
            "removed_nodes", dict, "Nodes removed since the previous version"
        )

        self.load()

    def load(self):
        versions = [
            AcousticTaxonomy.from_dict(self.values_asdict(values)) for values in self
        ]
        self.releases = versions[1:]
        if len(versions) > 0:
            self.draft = versions[0]

    def save(self, comment: str = None):
        self.draft.comment = comment
        self.draft.timestamp = datetime.now(timezone.utc)
        row = self.draft.to_dict()
        if len(self) == 0:
            self.add(row)
        else:
            self.set(0, row)

    def release(self, comment: str = None):
        super().release(comment)
        self.save()
        self.add(self.current.to_dict())
