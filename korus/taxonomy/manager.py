import numpy as np
import pandas as pd
from datetime import datetime, timezone
from .taxonomy import Taxonomy
from .acoustic import AcousticTaxonomy


class LabelManager:
    def __init__(
        self, columns: list[str] = ["tag", "identifier"], index: list[str] = ["tag"]
    ):
        self.columns = columns
        self.index = ["version"] + index
        self._df = None
        self._idf = None

    def get_label_id(
        self,
        indices: tuple | list[tuple],
        always_list: bool = False,
    ) -> np.int64 | np.ndarray:
        """

        Raises:
            ValueError: if an invalid index is passed
        """
        ndim = np.ndim(indices)
        if ndim == 1:
            indices = [indices]

        ids = []
        for idx in indices:
            idx = tuple([slice(None) if i == "*" else i for i in idx])
            try:
                id = self._idf.loc[idx, slice(None)].id

            except KeyError:
                err_msg = f"Failed to obtain Label ID because an invalid index was passed: {idx}"
                raise ValueError(err_msg)

            if isinstance(id, np.int64):
                ids.append(id.item())
            else:
                ids += id.values.tolist()

        if ndim == 1 and len(ids) == 1 and not always_list:
            ids = ids[0]

        return ids

    @property
    def df(self) -> pd.DataFrame:
        return self._df

    @df.setter
    def df(self, x: pd.DataFrame):
        self._df = x
        self._df = x.astype({"version": int})
        self._df.index.name = "id"

        self._idf = self._df.copy()
        self._idf["id"] = self._df.index.values
        self._idf.set_index(self.index, inplace=True)

    def update(self, version: int, rows: list[tuple]):
        null_row = tuple([None for _ in self.columns])
        data = [null_row] + rows
        new_df = pd.DataFrame(data, columns=self.columns, dtype=object)
        new_df["version"] = version
        if self.df is not None:
            new_df = pd.concat([self.df, new_df], ignore_index=True)

        self.df = new_df


class AcousticLabelManager(LabelManager):
    def __init__(self):
        columns = (
            [
                "sound_source_tag",
                "sound_source_id",
                "sound_type_tag",
                "sound_type_id",
            ],
        )
        index = [
            "sound_source_tag",
            "sound_type_tag",
        ]
        super().__init__(columns, index)


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
        label: str | tuple | list,
        version: int = None,
        ascend: bool = False,
        descend: bool = False,
        always_list: bool = False,
    ):
        tax = self.current if version is None else self.releases[version]

        return get_label_id(
            label,
            tax,
            self.labels,
            ascend,
            descend,
            always_list,
        )


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
