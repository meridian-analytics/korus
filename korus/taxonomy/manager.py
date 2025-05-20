import numpy as np
import pandas as pd
from datetime import datetime, timezone
from .taxonomy import Taxonomy
from .acoustic import AcousticTaxonomy
from .label import LabelManager, AcousticLabelManager


class TaxonomyManager:
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

    def __init__(self, tax: Taxonomy, labels: LabelManager):
        super().__init__()
        self.draft = tax
        self.releases = []
        self.labels = labels

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
        self.labels.update(release.version, release.all_labels)
        self.draft.clear_history()

    def get_label_id(
        self,
        label: str | tuple | list = None,
        version: int = None,
        ascend: bool = False,
        descend: bool = False,
        always_list: bool = False,
        label_id: int | list[int] = None,
    ) -> int | list[int]:
        """
        Args:
            label: str | tuple | list
                Sound source and sound type label(s). The character '*' can be used as wildcard.
                For example, use ('SRKW','*') to retrieve all label IDs associated with the sound
                source 'SRKW', irrespective of sound type. Multiple source-type pairs can be
                specified as a list of tuples. Ignored if `label_id` is provided.
            version: int
                Taxonomy version. If None, the current release is used.
            ascend: bool
                Also return the labels of ancestral nodes.
            descend: bool
                Also return the labels of descendant nodes.
            always_list: bool
                Whether to always return a list. Default is False.
            label_id: int | list[int]
                Label IDs. If provided, the `label` and `version` arguments are ignored.

        Returns:
            ids: int | list[int]
                Label identifier(s)

        Raises:
            ValueError: if the label does not exist in the taxonomy
        """
        # if `label_id` was specified, used it in place of `label`
        if label_id is not None:
            ids = []
            for version, label in self.labels.get_label(label_id, always_list=True):
                ids += self.get_label_id(
                    label, version, ascend, descend, always_list=True
                )

            if isinstance(label_id, int) and len(ids) == 1 and not always_list:
                ids = ids[0]

            return ids

        tax = self.current if version is None else self.releases[version - 1]

        ids = get_label_id(
            label,
            tax,
            self.labels,
            ascend,
            descend,
            always_list,
        )

        return ids

    def crosswalk_label_id(
        self,
        label_id: int | list[int],
        version: int = None,
        ascend: bool = False,
        descend: bool = False,
        always_list: bool = False,
        equiv: bool = False,
    ) -> int | list[int]:
        """Map a list of label IDs to another taxonomy version.

        TODO: implement this method

        Args:
            label_id: int | list[int]
                Label ID(s)
            version: int
                Destination taxonomy version
            ascend: bool
                Also return the labels of ancestral nodes of the mapped node(s).
            descend: bool
                Also return the labels of descendant nodes of the mapped node(s).
            equiv: bool
                If True, only return the mapped label IDs that are 1-to-1

        Returns:
            : int | list[int]
                The mapped label ID(s).
        """
        tax = self.current if version is None else self.releases[version - 1]

        pass

    def trace_node_history(
        self, node_id: str, version: int = None, bool=None, mode: str = "backward"
    ):
        """Maps a node in the taxonomy tree to any taxonomy version.

        TODO: implement this method

        Args:
            node_id: str
                Node identifier
            version: int
                Destination taxonomy version
            mode: str
                * backward/b: trace node history backwards in time (default)
                * forward/f: trace node history forward in time

        Returns:
            : list[str]
                Node identifiers in the destination taxonomy.
            is_equivalent: bool
                Whether the input node and the mapped node(s) may be considered equivalent.
        """
        pass


class AcousticTaxonomyManager(TaxonomyManager):
    def __init__(self):
        super().__init__(AcousticTaxonomy(), AcousticLabelManager())


def get_label_id(
    label: str | tuple | list,
    taxonomy: Taxonomy,
    label_manager: LabelManager,
    ascend: bool = False,
    descend: bool = False,
    always_list: bool = False,
) -> int | list[int]:
    """Returns the IDs of one or several labels.

    If @ascend is set to True, the function will also return the label IDs of all the
    ancestral nodes in the taxonomy tree. For example, if the sound source is specified as
    SRKW, it will return labels corresponding not only to SRKW, but also KW, Toothed,
    Cetacean, Mammal, Bio, and Unknown.

    If @descend is set to True, the function will also return the label ids of all the
    descendant nodes in the taxonomy tree. For example, if the sound source is specified
    as SRKW, it will return labels corresponding not only to SRKW, but also J, K, and L pod.

    Args:
        label: str | tuple | list
            Sound source and sound type label(s). The character '*' can be used as wildcard.
            For example, use ('SRKW','*') to retrieve all label IDs associated with the sound
            source 'SRKW', irrespective of sound type. Multiple source-type pairs can be
            specified as a list of tuples.
        taxonomy: Taxonomy
            The taxonomy
        label_manager: LabelManager
            The label manager
        ascend: bool
            Also return the labels of ancestral nodes.
        descend: bool
            Also return the labels of descendant nodes.
        always_list: bool
            Whether to always return a list. Default is False.

    Returns:
        id: int | list[int]
            Label identifier(s)

    Raises:
        ValueError: if the (sound-source, sound-type) label does not exist in the taxonomy
    """
    # recast the `label` argument as list[tuple]
    labels = [label] if not isinstance(label, list) else label
    labels = [l if isinstance(l, tuple) else (l,) for l in labels]

    # taxonomy version
    v = taxonomy.version

    # loop over labels and get ID of each
    ids = []
    for l0 in labels:
        ids += label_manager.get_label_id((v, *l0), always_list=True)

        if ascend:
            for l in taxonomy.ascend(*l0, include_start_node=False):
                ids += label_manager.get_label_id((v, *l), always_list=True)

        if descend:
            for l in taxonomy.descend(*l0, include_start_node=False):
                ids += label_manager.get_label_id((v, *l), always_list=True)

    # recast output
    if not always_list and len(ids) == 1:
        ids = ids[0]

    return ids
