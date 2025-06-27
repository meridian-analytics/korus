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

        return super().add(row)

    def get_id(self, deployment_id: int | list[int], filename: str | list[str]) -> list[int]:
        """Given the deployment ID and the file names, get the file IDs

        Args:
            deployment_id: int | list[int]
                Deployment ID
            filename: str | list[str]
                File name(s)

        Returns:
            indices: list[int]
                File IDs. If a filename is not found in the database, its ID is set to None.
        """
        # convert to lists
        filenames = [filename] if isinstance(filename, str) else filename
        deployment_ids = [deployment_id for _ in filenames] if isinstance(deployment_id, int) else deployment_id

        assert_msg = "deployment_id and filename have incompatible shapes"
        assert len(filenames) == len(deployment_ids), assert_msg

        # query files one at the time
        indices = []
        for deploy_id, fname in zip(deployment_ids, filenames):
            cond = {"deployment_id": deploy_id, "filename": fname}
            idx = self.reset_filter().filter(cond).indices
            idx = idx[0] if len(idx) > 0 else None
            indices.append(idx)

        return indices

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
        return [float(n / sr) for n, sr in data]

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
