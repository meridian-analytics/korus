import os
from datetime import datetime, timedelta
from korus.database.backend import TableBackend
from .interface import TableInterface
from .storage import StorageInterface


class FileInterface(TableInterface):
    """Defines the interface of the File Table."""

    def __init__(self, backend: TableBackend, storage: StorageInterface):
        super().__init__("file", backend)

        self._storage = storage

        self.add_field("deployment_id", int, "Deployment index")
        self.add_field("storage_id", int, "Storage index")
        self.add_field("filename", str, "Filename")
        self.add_field("relative_path", str, "Directory path", default="")
        self.add_field("sample_rate", int, "Sampling rate in Hz")
        self.add_field("num_samples", int, "Number of samples")
        self.add_field(
            "start_utc",
            datetime,
            "Start time of recording (UTC)",
            required=False,
        )
        self.add_field(
            "end_utc",
            datetime,
            "End time of recording (UTC) [inferred from the start time, number of samples, and sampling rate]",
            required=False,
        )
        self.add_field("format", str, "Audio format", required=False)
        self.add_field("codec", str, "Audio codec", required=False)

    def add(self, row: dict):
        """Add an entry to the table.

        Args:
            row: dict
                Input data in the form of a dict, where the keys are the field names
                and the values are the values to be added to the database.
        """
        if "start_utc" in row:
            row["end_utc"] = row["start_utc"] + timedelta(
                microseconds=row["num_samples"] / row["sample_rate"] * 1e6
            )

        super().add(row)

    def get_duration(self, indices: int | list[int]) -> list[float]:
        """Get file duration.

        Args:
            indices: int | list[int]
                The indices of the entries to be returned. If None, all entries in the table are returned.

        Returns:
            : list[float]
                File duration(s) in seconds.
        """
        data = self.get(indices=indices, fields=["num_samples", "sample_rate"])
        return [float(n * sr) for n, sr in data]

    def get_absolute_path(self, indices: int | list[int]) -> list[str]:
        """Get the absolute paths to the audio files.

        Args:
            indices: int | list[int]
                The indices of the entries to be returned. If None, all entries in the table are returned.

        Returns:
            : list[str]
                Absolute file paths.
        """
        data = self.get(
            indices=indices, fields=["storage_id", "filename", "relative_path"]
        )
        storage_ids, filenames, rel_paths = zip(*data)
        top_paths = self._storage.get(storage_ids, ["path"], always_tuple=False)
        return [
            os.path.join(top_path, rel_path, filename)
            for top_path, rel_path, filename in zip(top_paths, rel_paths, filenames)
        ]
