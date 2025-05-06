from .interface import TableInterface
from korus.taxonomy import AcousticTaxonomy, VersionedAcousticTaxonomy
from datetime import datetime, timezone


class TaxonomyInterface(TableInterface, VersionedAcousticTaxonomy):
    """Defines the interface of the Taxonomy Table.
    
    Row 0 is used for storing the draft version.
    The first release is stored in row 1, the second release in row 2, etc.

    TODO: consider removing the `name` field
    """

    def __init__(self, backend):
        super().__init__(name="taxonomy", backend=backend)

        self.add_field("name", str, "Name")
        self.add_field("version", int, "Version", required=False)
        self.add_field("timestamp", datetime, "Time of release (UTC)")
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
        """Load all taxonomy releases including draft version from the database into memory.
        """
        versions = [
            AcousticTaxonomy.from_dict(self.values_asdict(values)) for values in self
        ]
        self.releases = versions[1:]
        if len(versions) > 0:
            self.draft = versions[0]

    def save(self, comment: str = None):
        """Save the current draft to the database.

        Note: Overwrites the existing draft.

        Args:
            comment: str (optional)
                An explanatory note
        """
        self.draft.comment = comment
        self.draft.timestamp = datetime.now(timezone.utc)
        row = self.draft.to_dict()
        if len(self) == 0:
            self.add(row)
        else:
            self.set(0, row)

    def release(self, comment: str = None):
        """Release a new version of the taxonomy and save it to the database
        
        Args:
            comment: str (optional)
                An explanatory note
        """
        super().release(comment)
        self.save()
        self.add(self.current.to_dict())
