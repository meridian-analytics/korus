import numpy as np
import pandas as pd


class LabelManager:
    """Helper class for managing taxonomy labels.

    When combined with the taxonomy version, the tag/identifiers
    must allow each label to be uniquely identified.

    Args:
        tags: list[str]
            The names of the node tags.
        nids: list[str]
            The names of the node IDs.

    Attrs & Properties:
        df: pd.DataFrame
            DataFrame with all labels, indexed by the label ID, named `id`
    """

    def __init__(self, tags: list[str] = ["tag"], nids=["identifier"]):
        self._cols = {"tag": tags, "nid": nids}

        # DataFrame with all labels, indexed by the label ID,
        # i.e., elements may be accessed as self._df.loc[id]
        self._df = None

        # DataFrame with all labels, indexed by the version number
        # and tags or identifiers; elements may be accessed as
        # self._idf[k].loc[(version,*self._cols[k])]
        self._idf = {k: None for k in self._cols.keys()}

    @property
    def columns(self):
        return self._cols["tag"] + self._cols["nid"]

    def get_label(
        self,
        id: int | list[int],
        return_nid: bool = False,
        always_list: bool = False,
        return_version: bool = True,
    ) -> tuple | list[tuple]:
        """TODO: docstring"""
        if id is None:
            return None

        cols = ["version"]
        if return_nid:
            cols += self._cols["nid"]
        else:
            cols += self._cols["tag"]

        ids = id if isinstance(id, list) else [id]
        rows = self.df.loc[ids][cols].values
        res = [(row[0], tuple(row[1:])) for row in rows]

        if not return_version:
            res = [x[1] for x in res]

        if np.ndim(id) == 0 and not always_list:
            res = res[0]

        return res

    def has_nid(self, nid: str | tuple[str], version: int = None):
        if version and isinstance(nid, tuple):
            return (version, *nid) in self._idf["nid"].index

        else:
            df = self.df if version is None else self.df[self.df.version == version]
            return np.any(df[self._cols["nid"]].values == nid)

    def get_label_id(
        self,
        version: int | list[int],
        tag: tuple | list[tuple] = None,
        nid: tuple | list[tuple] = None,
        always_list: bool = False,
    ) -> np.int64 | np.ndarray:
        """Get label ID.

        Args:
            version: int | list[int]
                Taxonomy version(s). If list, must have the same length as `tag` or `nid`
            tag: tuple | list[tuple]
                Tag(s).
            nid: tuple | list[tuple]
                Node ID(s).
            always_list: bool
                Whether to always return a list of ints.

        Returns:
            : np.int64 | np.ndarray
                The label ID(s)

        Raises:
            ValueError: if an invalid index is passed
        """
        if tag is None and nid is None:
            return None

        if nid is None:
            indices = tag
            df = self._idf["tag"]
        else:
            indices = nid
            df = self._idf["nid"]

        ndim = np.ndim(indices)
        if ndim == 0:
            indices = [(indices,)]
        elif ndim == 1:
            indices = [indices]

        if np.ndim(version) == 0:
            version = [version for _ in range(len(indices))]

        indices = [(v, *idx) for v, idx in zip(version, indices)]

        ids = []
        for idx in indices:
            idx = tuple([slice(None) if i == "*" else i for i in idx])
            try:
                id = df.loc[idx, slice(None)].id

            except KeyError:
                err_msg = f"Failed to obtain Label ID because an invalid index was passed: {idx}"
                raise ValueError(err_msg)

            if isinstance(id, np.int64):
                ids.append(id.item())
            else:
                ids += id.values.tolist()

        if ndim <= 1 and len(ids) == 1 and not always_list:
            ids = ids[0]

        return ids

    @property
    def df(self) -> pd.DataFrame:
        return self._df

    @df.setter
    def df(self, x: pd.DataFrame):
        if x is None:
            return

        self._df = x
        self._df = x.astype({"version": int})
        self._df.index.name = "id"

        # also update the indexed DataFrames
        for key, cols in self._cols.items():
            index = ["version"] + cols
            self._idf[key] = self._df.copy()
            self._idf[key]["id"] = self._df.index.values
            self._idf[key].set_index(index, inplace=True)

    def update(self, version: int, rows: list[tuple]):
        new_df = pd.DataFrame(rows, columns=self.columns, dtype=object)
        new_df["version"] = version
        if self.df is not None:
            new_df = pd.concat([self.df, new_df], ignore_index=True)

        self.df = new_df


class AcousticLabelManager(LabelManager):
    def __init__(self):
        super().__init__(
            tags=["sound_source_tag", "sound_type_tag"],
            nids=["sound_source_id", "sound_type_id"],
        )
