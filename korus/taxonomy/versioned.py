from .taxonomy import Taxonomy
from .acoustic import AcousticTaxonomy


class VersionedTaxonomy:

    def __init__(self, tax: Taxonomy):
        self.draft = tax
        self.published = []

    @property
    def version(self):
        return len(self.published)

    @property
    def current(self):
        if len(self.published) == 0:
            return None

        else:
            return self.published[-1]

    def publish(self):
        self.published.append(self.draft)


class VersionedAcousticTaxonomy(VersionedTaxonomy):
    def __init__(self):
        super().__init__(AcousticTaxonomy())
