from .interface import FieldDefinition, TableInterface, _create_field_definitions
from dataclasses import dataclass

@dataclass
class FileRow:
    """ Row format of the File Table Interface.
    
    Used for passing data between the table interface and the database backend.
    """
    deployment_id: int
    storage_id: int
    filename: str
    relative_path: str
    sample_rate: int

""" Human-readable, descriptions of the field in the FileRow definition
"""
_field_descriptions = {
    "deployment_id": "Deployment index",
    "storage_id": "Storage index",
    "filename": "Filename",
    "relative_path": "Directory path",
    "sample_rate": "Sampling rate in Hz",
}

class FileInterface(TableInterface):
    """ Defines the interface of the File Table.
    
    
    """
    def __init__(self, backend):
        super().__init__("file")

        self.backend = backend
        self._fields = _create_field_definitions(FileRow, _field_descriptions)

    @property
    def fields(self) -> list[FieldDefinition]:
        """ The definitions of the table's fields"""
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
        # collect submitted data in a FileRow 
        row = FileRow(
            deployment_id,
            storage_id,
            filename,
            relative_path,
            sample_rate,
        )
        # and pass it to the backend
        return self.backend.add(row, **kwargs)

#    def _add_row(row: FileRow, **kwargs) -> int:
#        """ Insert row of data into the database """
#        raise NotImplementedError("Must be implemented in child class")

    def get(self, indices=None, fields=None, as_dataframe=False, **kwargs):
        return self.backend.get(indices, fields, **kwargs)

#    def _get_rows(indices=None, fields=None, **kwargs) -> list[FileRow]:
#        """ Retrieve rows of data from the database """
#        raise NotImplementedError("Must be implemented in child class")

    def filter(self):
        pass