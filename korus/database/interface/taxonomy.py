from .interface import TableInterface
from .label import LabelInterface
from korus.taxonomy import AcousticTaxonomy, AcousticTaxonomyManager
from korus.database.backend import TableBackend
from datetime import datetime, timezone
import pandas as pd


class TaxonomyInterface(TableInterface, AcousticTaxonomyManager):
    """Defines the interface of the Taxonomy Table.

    Row 0 is used for storing the draft version.
    The first release is stored in row 1, the second release in row 2, etc.

    TODO: consider removing the `name` field
    """

    def __init__(self, backend: TableBackend, label_interface: LabelInterface):
        super().__init__(name="taxonomy", backend=backend)

        self.label_interface = label_interface

        self._create_field("name", str, "Name")
        self._create_field("version", int, "Version", required=False)
        self._create_field("timestamp", datetime, "Time of release (UTC)")
        self._create_field("changes", list, "Changes since previous version")
        self._create_field(
            "comment", str, "Any additional release notes", required=False
        )
        self._create_field("tree", dict, "Taxonomy tree")
        self._create_field(
            "created_nodes", dict, "Nodes created since the previous version"
        )
        self._create_field(
            "removed_nodes", dict, "Nodes removed since the previous version"
        )

        self.load()

    def load(self):
        """Load all taxonomy releases including draft version from the database into memory.

        Also load the label mapping.
        """
        versions = [
            AcousticTaxonomy.from_dict(self.values_asdict(values)) for values in self
        ]

        self.releases = versions[1:]
        if len(versions) > 0:
            self.draft = versions[0]

        # load label data
        fields = self.label_interface.names
        data = self.label_interface.get(fields=fields, return_indices=True)
        df = pd.DataFrame(data, columns=["id"] + fields)
        df = df.rename(columns={"taxonomy_id": "version"})
        self.labels.df = df.set_index("id")

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
            self.update(0, row)

    def release(self, comment: str = None):
        """Release a new version of the taxonomy and save it to the database

        Args:
            comment: str (optional)
                An explanatory note
        """
        super().release(comment)
        self.save()
        self.add(self.current.to_dict())

        # also save the labels
        df = self.labels.df[self.labels.df.version == self.version]
        for idx, row in df.iterrows():
            row["taxonomy_id"] = row.pop("version")
            self.label_interface.add(row)
