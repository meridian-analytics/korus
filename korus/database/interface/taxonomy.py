from .interface import TableInterface
from korus.taxonomy import VersionedAcousticTaxonomy
from datetime import datetime


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
        versions = []
        for row in self:
            pass
            #TODO: convert row -> AcousticTaxonomy;  then versions.append(tax)

        if len(versions) > 1:       
            self.releases = versions[:-1]
        
        if len(versions) > 0:
            self.draft = versions[-1]

    def save(self):
        pass
        #TODO: save draft to database
        #row = {...}
        #self.set(idx, row)

    def release(self, comment=None):
        super().release()
        self.save()
        #TODO: save new released version to database
        #row = {...}
        #self.add(row)
