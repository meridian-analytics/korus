from .interface import TableInterface
from korus.taxonomy import AcousticTaxonomy, VersionedAcousticTaxonomy
from datetime import datetime


def _taxonomy_from_row(row) -> AcousticTaxonomy:
    #TODO: implement this
    pass

def _row_from_taxonomy(tax, version, comment=None) -> dict:
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
        versions = [_taxonomy_from_row(row) for row in self]

        if len(versions) > 1:       
            self.releases = versions[:-1]
        
        if len(versions) > 0:
            self.draft = versions[-1]

    def save(self):
        row = _row_from_taxonomy(self.draft)

        if len(self) == 0:
            self.add(row)
        
        else:
            idx = len(self) - 1
            self.set(idx, row)

    def release(self, comment=None):
        super().release()

        # add new release to table (overwrite current draft)
        idx = len(self)-1
        row = _row_from_taxonomy(self.current, comment=comment)
        self.set(idx, row)

        # add new `draft` entry in table
        row = _row_from_taxonomy(self.draft)
        self.add(row)
