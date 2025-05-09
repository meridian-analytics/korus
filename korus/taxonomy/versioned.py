import pandas as pd
from datetime import datetime, timezone
from .taxonomy import Taxonomy
from .acoustic import AcousticTaxonomy


class VersionedTaxonomy:
    """Class for managing `evolving` taxonomies.

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
        self.labels = None

    @property
    def version(self) -> int:
        """The current version number"""
        return len(self.releases)

    @property
    def current(self) -> Taxonomy:
        """The current version (latest release) of the taxonomy"""
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
        self._update_labels(release)
        self.draft.clear_history()

    def _update_labels(self, release: Taxonomy):
        data = [(None, None, None, None)] + release.all_labels
        columns = [
            "sound_source_tag",
            "sound_source_id",
            "sound_type_tag",
            "sound_type_id",
        ]
        new_labels = pd.DataFrame(data, columns=columns, dtype=object)
        new_labels["version"] = release.version
        if self.labels is None:
            self.labels = new_labels
        else:
            self.labels = pd.concat([self.labels, new_labels], ignore_index=True)

        self.labels = self.labels.astype({"version": int})
        self.labels.index.name = "id"


class VersionedAcousticTaxonomy(VersionedTaxonomy):
    def __init__(self):
        super().__init__(AcousticTaxonomy())
