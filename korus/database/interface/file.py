from .interface import FieldDefinition, TableInterface, _create_field_definitions
from dataclasses import dataclass

@dataclass
class FileRow:
    deployment_id: int
    storage_id: int
    filename: str
    relative_path: str
    sample_rate: int

_field_descriptions = {
    "deployment_id": "Deployment index",
    "storage_id": "Storage index",
    "filename": "Filename",
    "relative_path": "Directory path",
    "sample_rate": "Sampling rate in Hz",
}

class FileInterface(TableInterface):
    def __init__(self):
        super().__init__("file")

        self._fields = _create_field_definitions(FileRow, _field_descriptions)

    @property
    def fields(self) -> list[FieldDefinition]:
        return self._fields
    
    def add(
        self, 
        deployment_id: int,
        storage_id: int,
        filename: str,
        relative_path: str,
        sample_rate: str,
        **kwargs,
    ) -> int:
        row = FileRow(
            deployment_id,
            storage_id,
            filename,
            relative_path,
            sample_rate,
        )
        return self._add_row(row, **kwargs)

    def _add_row(row: FileRow, **kwargs) -> int:
        pass

    def get(self, indices=None, fields=None, as_dataframe=False, **kwargs):
        return self._get_rows(indices, fields, **kwargs)

    def _get_rows(indices=None, fields=None, **kwargs) -> list[FileRow]:
        pass

    def filter(self):
        pass