from .interface import TableInterface
from korus.taxonomy import VersionedAcousticTaxonomy


class TaxonomyInterface(TableInterface, VersionedAcousticTaxonomy):
    """Defines the interface of the File Table."""

    def __init__(self, backend):
        super().__init__("taxonomy", backend)
