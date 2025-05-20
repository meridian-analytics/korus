import numpy as np
import pandas as pd


class LabelManager:
    """Helper class for managing taxonomy labels.

    When combined with the taxonomy version, the tag/identifier columns
    must allow each label to be uniquely identified.

    Args:
        tags: list[str]
            The columns used for storing node tags.
        ids: list[str]
            The columns used for storing node identifiers.

    Attrs & Properties:
        df: pd.DataFrame
            DataFrame with all labels, indexed by the label ID
    """

    def __init__(self, tags: list[str] = ["tag"], ids=["identifier"]):
        self._cols = {"tag": tags, "id": ids}

        # DataFrame with all labels, indexed by the label ID,
        # i.e., elements may be accessed as self._df.loc[id]
        self._df = None

        # DataFrame with all labels, indexed by the version number
        # and tags or identifiers; elements may be accessed as
        # self._idf[k].loc[(version,*self._cols[k])]
        self._idf = {k: None for k in self._cols.keys()}

    @property
    def columns(self):
        return self._cols["tag"] + self._cols["id"]

    def get_label(
        self,
        id: int | list[int],
        node_id: bool = False,
        always_list: bool = False,
    ) -> tuple | list[tuple]:
        """TODO: docstring"""
        cols = ["version"]
        if node_id:
            cols += self._cols["id"]
        else:
            cols += self._cols["tag"]

        ids = id if isinstance(id, list) else [id]
        rows = self.df.loc[ids][cols].values
        res = [(row[0], tuple(row[1:])) for row in rows]
        if np.ndim(id) == 0 and not always_list:
            res = res[0]

        return res

    def get_label_id(
        self,
        indices: tuple | list[tuple],
        node_id: bool = False,
        always_list: bool = False,
    ) -> np.int64 | np.ndarray:
        """Get label ID.

        Args:
            indices: tuple | list[tuple]
                Multi-level index, with the version as the first-level index.
            node_id: bool
                Query on node identifiers instead of tags.
            always_list: bool
                Whether to always return a list of ints.

        Returns:
            : np.int64 | np.ndarray
                The label ID(s)

        Raises:
            ValueError: if an invalid index is passed
        """
        ndim = np.ndim(indices)
        if ndim == 1:
            indices = [indices]

        key = "id" if node_id else "tag"

        ids = []
        for idx in indices:
            idx = tuple([slice(None) if i == "*" else i for i in idx])
            try:
                id = self._idf[key].loc[idx, slice(None)].id

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
        null_row = tuple([None for _ in self.columns])
        data = [null_row] + rows
        new_df = pd.DataFrame(data, columns=self.columns, dtype=object)
        new_df["version"] = version
        if self.df is not None:
            new_df = pd.concat([self.df, new_df], ignore_index=True)

        self.df = new_df


class AcousticLabelManager(LabelManager):
    def __init__(self):
        super().__init__(
            tags=["sound_source_tag", "sound_type_tag"],
            ids=["sound_source_id", "sound_type_id"],
        )
