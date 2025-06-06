import numpy as np
from korus.database.backend import TableBackend


class InMemoryTableBackend(TableBackend):
    """Stores data as a list of dicts.

    Attrs:
        rows: list[dict]
            The rows of data added to the database
        fields: list[str]
            The field/column names
    """

    def __init__(self):
        super().__init__("in_memory")
        self.rows = []
        self.fields = []
        self.reset_cursor()

    def __len__(self):
        return len(self.rows)

    def reset_cursor(self):
        self._idx = -1

    def __next__(self):
        self._idx += 1
        if self._idx >= len(self):
            raise StopIteration

        return self._idx

    def get(self, indices=None, fields=None, return_indices=False):
        if len(self.rows) == 0:
            return []

        if indices is None:
            indices = [i for i in range(len(self.rows))]

        if fields is None:
            fields = self.fields

        if np.ndim(indices) == 0:
            indices = [indices]

        if np.ndim(fields) == 0:
            fields = [fields]

        if return_indices:
            fields = ["id"] + fields
            rows = self.rows.copy()
            for i in range(len(rows)):
                rows[i]["id"] = i
        else:
            rows = self.rows

        return [
            tuple([row.get(field, None) for field in fields])
            for idx, row in enumerate(rows)
            if idx in indices
        ]

    def add(self, row):
        self.fields += [k for k in row.keys() if k not in self.fields]
        self.rows.append(row)

    def remove(self, indices=None):
        if isinstance(indices, int):
            indices = [indices]

        if indices is None:
            self.rows = []

        else:
            indices = sorted(indices, reverse=True)
            for idx in indices:
                del self.rows[idx]

    def set(self, idx, row):
        self.rows[idx] = row

    def filter(self, *conditions, indices=None, **_):
        filtered_indices = []
        for idx, row in enumerate(self.rows):

            results = []
            for condition in conditions:

                result = True
                for name, values in condition.items():
                    if name[-1] == "~":
                        name = name[:-1]
                        invert = True
                    else:
                        invert = False

                    if name not in row:
                        continue

                    xs = row[name]

                    if not isinstance(xs, (list, tuple)):
                        xs = [xs]

                    if not isinstance(values, (tuple, list)):
                        values = [values]

                    # accept, if any element in xs fulfills the condition
                    for x in xs:
                        accept_x = False

                        if isinstance(values, tuple):
                            a, b = values
                            if invert:
                                accept_x = x < a or x > b
                            else:
                                accept_x = x >= a and x <= b

                        else:
                            if invert:
                                accept_x = x not in values
                            else:
                                accept_x = x in values

                        if accept_x:
                            break

                    # logical AND
                    result *= accept_x

                results.append(result)

            # logical OR
            if np.sum(results) > 0:
                filtered_indices.append(idx)

        if indices is not None:
            filtered_indices = [idx for idx in filtered_indices if idx in indices]

        return filtered_indices


class InMemoryJobBackend(InMemoryTableBackend):
    def __init__(self):
        super().__init__()
        self.files = []

    def add_file(self, job_id: int, file_id: int, channel: int = 0):
        self.files.append((job_id, file_id, channel))

    def get_files(self, job_id: int | list[int]) -> list[tuple[int, int]]:
        if isinstance(job_id, int):
            job_id = [job_id]

        return [row[1:] for row in self.files if row[0] in job_id]
