from datetime import datetime, timezone
from .taxonomy import Taxonomy
from .acoustic import AcousticTaxonomy


class VersionedTaxonomy:
    """ Class for managing `evolving` taxonomies.

    Args:
        tax: korus.taxonomy.Taxonomy
            The initial draft. Can be empty.

    Attrs:
        draft: korus.taxonomy.Taxonomy
            The draft taxonomy version
        releases: list[korus.taxonomy.Taxonomy]
            The released taxonomy versions    
    """

    def __init__(self, tax: Taxonomy):
        super().__init__()
        self.draft = tax
        self.releases = []

    @property
    def version(self) -> int:
        """ The current version number"""
        return len(self.releases)

    @property
    def current(self) -> Taxonomy:
        """ The current version (latest release) of the taxonomy"""
        if len(self.releases) == 0:
            return None

        else:
            return self.releases[-1]

    def release(self, comment: str = None):
        """Release a new version of the taxonomy.
        
        Increments the version number by +1.
        The current OS clock time is used to timestamp the release.

        Args:
            comment: str (optional)
                An explanatory note
        """
        release = self.draft.deepcopy()
        release.version = self.version + 1
        release.comment = comment
        release.timestamp = datetime.now(timezone.utc)
        self.releases.append(release)
        self.draft.clear_history()



class VersionedAcousticTaxonomy(VersionedTaxonomy):
    def __init__(self):
        super().__init__(AcousticTaxonomy())
