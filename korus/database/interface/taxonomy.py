from .interface import TableInterface
from korus.taxonomy import AcousticTaxonomy, VersionedAcousticTaxonomy
from datetime import datetime


def _tax_from_row(row) -> AcousticTaxonomy:
    #TODO: implement this
    pass

def _row_from_tax(tax, version, comment=None) -> dict:
    #TODO: implement this
    return {
        "version": version,
        "comment": comment,
        #etc.
    }


class TaxonomyInterface(TableInterface, VersionedAcousticTaxonomy):
    """Defines the interface of the Taxonomy Table."""

    def __init__(self, backend):
        super().__init__("taxonomy", backend)

        self.add_field("version", int, "Version")
        self.add_field("timestamp", datetime, "Time of release")
        self.add_field("changes", list, "Changes since previous version")
        self.add_field("comment", str, "Any additional release notes")
        self.add_field("tree", dict, "Taxonomy tree")
        self.add_field("created_nodes", dict, "Nodes created since the previous version")
        self.add_field("removed_nodes", dict, "Nodes removed since the previous version")

        self.load()

    def load(self):
        versions = [_tax_from_row(row) for row in self]
        self.releases = versions[1:]
        if len(versions) > 0:
            self.draft = versions[0]

    def save(self):
        row = _row_from_tax(self.draft)
        self.set(0, row)

    def release(self, comment=None):
        super().release()
        row = _row_from_tax(self.current, version=self.version, comment=comment)
        self.add(row)
        self.save()
