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
        self._label_columns = ["tag", "identifier"]

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
        null_row = tuple([None for _ in self._label_columns])
        data = [null_row] + release.all_labels
        new_labels = pd.DataFrame(data, columns=self._label_columns, dtype=object)
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
        self._label_columns = [
            "sound_source_tag",
            "sound_source_id",
            "sound_type_tag",
            "sound_type_id",
        ]


'''
def get_label_id(
    versioned,
    source_type=None,
    taxonomy_id=None,
    ascend=False,
    descend=False,
    always_list=False,
):
    """Returns the label identifier corresponding to a sound-source, sound-type tag tuple.

    If @ascend is set to True, the function will also return the label ids of all the
    ancestral nodes in the taxonomy tree. For example, if the sound source is specified as
    SRKW, it will return labels corresponding not only to SRKW, but also KW, Toothed,
    Cetacean, Mammal, Bio, and Unknown.

    If @descend is set to True, the function will also return the label ids of all the
    descendant nodes in the taxonomy tree. For example, if the sound source is specified
    as SRKW, it will return labels corresponding not only to SRKW, but also J, K, and L pod.

    Args:
        conn: sqlite3.Connection
            Database connection
        source_type: tuple(str, str) or list(tuple)
            Sound source and sound type tags. The character '%' can be used as wildcard.
            For example, use ('SRKW','%') to retrieve all labels associated with the sound
            source 'SRKW', irrespective of sound type. Multiple source-type pairs can be
            specified as a list of tuples.
        taxonomy_id: int
            Acoustic taxonomy unique identifier. If not specified, the latest taxonomy will
            be used.
        ascend: bool
            Also return the labels of ancestral nodes.
        descend: bool
            Also return the labels of descendant nodes.
        always_list: bool
            Whether to always return a list. Default is False.

    Returns:
        id: int, list(int)
            Label identifier(s)

    Raises:
        ValueError: if a label with the specified @source_type does not exist
    """
    c = conn.cursor()

    tax, taxonomy_id = get_taxonomy(conn, taxonomy_id, return_id=True)  # load taxonomy

    if source_type == None:
        source_type = (None, None)

    if isinstance(source_type, tuple):
        source_type = [source_type]

    def _condition(name, val):
        "helper function for forming SQLite conditions"
        if val is None:
            return f"{name} IS NULL"
        else:
            return f"{name} LIKE '{val}'"

    where_condition = f"taxonomy_id = {taxonomy_id} AND ("
    for i, (sound_source, sound_type) in enumerate(source_type):
        # SQLite WHERE conditions
        ss_con = _condition("sound_source_tag", sound_source)
        st_con = _condition("sound_type_tag", sound_type)

        # check that the source,type tuple is valid
        query = f"""
            SELECT 
                id
            FROM 
                label
            WHERE 
                taxonomy_id = {taxonomy_id} AND {ss_con} AND {st_con}
        """
        rows = c.execute(query).fetchall()
        if len(rows) == 0:
            raise ValueError(
                f"'{sound_source,sound_type}' does not exist in taxonomy with id {taxonomy_id}"
            )

        if i > 0:
            where_condition += " OR "

        where_condition += f"({ss_con} AND {st_con})"

        # reverse search, continue to the root
        if ascend and sound_source not in ["%"]:
            gen = tax.ascend(sound_source, sound_type, include_start_node=False)
            for i, (s, t) in enumerate(
                gen
            ):  # generator returns (source,type) tag tuple
                ss_con = _condition("sound_source_tag", s)
                st_con = _condition("sound_type_tag", t)
                where_condition += f" OR ({ss_con} AND {st_con})"

        # forward search, continue to the leaves
        if descend and sound_source not in ["%"]:
            gen = tax.descend(sound_source, sound_type, include_start_node=False)
            for i, (s, t) in enumerate(
                gen
            ):  # generator returns (source,type) tag tuple
                ss_con = _condition("sound_source_tag", s)
                st_con = _condition("sound_type_tag", t)
                where_condition += f" OR ({ss_con} AND {st_con})"

    where_condition += ")"

    # SQLite query
    query = f"""
        SELECT 
            id
        FROM 
            label
        WHERE 
            {where_condition}
    """
    rows = c.execute(query).fetchall()
    id = [r[0] for r in rows]

    if not always_list and len(id) == 1:
        id = id[0]

    return id
'''