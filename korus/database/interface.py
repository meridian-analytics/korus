import inspect
from dataclasses import dataclass
from tabulate import tabulate


@dataclass
class Field:
    name: str 
    type: type
    default: 'typing.Any'
    description: str


def _create_field_definitions(arg_spec: inspect.FullArgSpec, descriptions: list[str]):
    """ Helper function for creating field definitions for table interfaces.
    
    Args:
        arg_spec: inspect.FullArgSpec
            Names, types, and default values of the parameters of the table interface's `add` method
            obtained using the `inspect` package's `fullargspec` function
        descriptions: list[str]
            Parameter descriptions. 

    Returns:
        fields: list[Field]
            Field definitions
    """
    n_fields = len(arg_spec.args)
    n_defaults = 0 if arg_spec.defaults is None else len(arg_spec.defaults)
    fields = []
    for i,name in enumerate(arg_spec.args[1:]):
        j = n_defaults - n_fields + i
        default = None if j < 0 else arg_spec.defaults[j]
        typ = arg_spec.annotations[name]
        fields.append(Field(name, typ, default, descriptions[i]))

    return fields
                      

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

        arg_spec = inspect.getfullargspec(FileInterface.add)

        descriptions = [
            "Deployment index",
            "Storage index",
            "Filename",
            "Directory path",
            "Sampling rate in Hz"
        ]

        self._fields = _create_field_definitions(arg_spec, descriptions)

    @property
    def fields(self) -> list[Field]:
        return self._fields
    
    def add(
        self, 
        deployment_id: int,
        storage_id: int,
        filename: str,
        relative_path: str,
        sample_rate: str,
    ):
        pass

class JobInterface(TableInterface):
    def __init__(self):
        super().__init__("job")

    def link_file(self, file_id: int):
        pass

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