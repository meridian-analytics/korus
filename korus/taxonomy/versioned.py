from .taxonomy import Taxonomy
from .acoustic import AcousticTaxonomy


class VersionedTaxonomy:

    def __init__(self, tax: Taxonomy):
        self.draft = tax
        self.releases = []

    @property
    def version(self):
        return len(self.releases)

    @property
    def current(self):
        if len(self.releases) == 0:
            return None

        else:
            return self.releases[-1]

    def release(self):
        self.releases.append(self.draft)


class VersionedAcousticTaxonomy(VersionedTaxonomy):
    def __init__(self):
        super().__init__(AcousticTaxonomy())
