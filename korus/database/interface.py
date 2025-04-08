from dataclasses import dataclass
from tabulate import tabulate


@dataclass
class Field:
    name: str 
    type: type
    default: 'typing.Any'
    description: str


class TableInterface:
    def __init__(self, name: str):
        self.name = name

    @property
    def fields(self) -> list[Field]:
        pass

    def filter(self):
        #search
        pass

    def get(self):
        #retrive entries
        pass

    def add(self):
        #add a new entry
        pass

    def __iter__(self):
        # iterates through the table rows
        pass

    def __str__(self) -> str:
        # table name
        res = f"Table name: {self.name}"

        # fields
        field_info = [
            (c.name, c.type.__name__, c.default, c.description) for c in self.fields
        ]
        res += "\nFields:\n"
        res += tabulate(
            field_info,
            headers = ["Name", "Type", "Default Value", "Description"]
        )
        
        return res

class DeploymentInterface(TableInterface):
    def __init__(self):
        super().__init__("deployment")

class AnnotationInterface(TableInterface):
    def __init__(self):
        super().__init__("annotation")

class FileInterface(TableInterface):
    def __init__(self):
        super().__init__("file")

        self._fields = [
            Field("deployment_id", int, None, "Deployment index"),
            Field("storage_id", int, None, "Storage index"),
            Field("filename", str, None, "Filename"),
            Field("relative_path", str, None, "Directory path"),
            Field("sample_rate", int, None, "Sampling rate in Hz"),
        ]

    @property
    def fields(self) -> list[Field]:
        return self._fields

class JobInterface(TableInterface):
    def __init__(self):
        super().__init__("job")

class StorageInterface(TableInterface):
    def __init__(self):
        super().__init__("storage")


class DatabaseInterface:
    @property
    def deployment(self) -> DeploymentInterface:
        pass

    @property
    def annotation(self) -> AnnotationInterface:
        pass

    @property
    def file(self) -> FileInterface:
        pass

    @property
    def job(self) -> JobInterface:
        pass

    @property
    def storage(self) -> StorageInterface:
        pass

    def __str__(self):
        """Prints a pretty summary of the database structure"""
        pass