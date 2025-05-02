from dataclasses import dataclass
from tabulate import tabulate
from korus.util import not_impl_err_msg


@dataclass
class FieldDefinition:
    """ Definition of a field in a Korus table interface.

    Fields must have one of the following Python types:

        bool,str,int,float,datetime
    
    Attrs:
        name: str
            The name of the field
        type: type
            The field Python type. 
        description: str
            Short, human-readable description of the data stored in this field
        default: same as type (optional)
            The field default value
    """
    name: str 
    type: type
    description: str
    default: 'typing.Any'


class TableInterface:
    """ Base class for all Korus table interfaces.
    
    Table interfaces define how users interact with the database, e.g., 
    adding data to the database or retrieving data from it.

    Args:
        name: str
            The table interface name
        backend: korus.database.backend.TableBackend
            The table backend, connecting the interface to the underlying database
    """
    def __init__(self, name: str, backend):
        self.name = name
        self.backend = backend
        self._fields = []

    @property
    def fields(self) -> list[FieldDefinition]:
        """ The definitions of the table's fields"""
        return self._fields
    
    @property
    def fields_asdict(self) -> dict[str, FieldDefinition]:
        """ The definitions of the table's fields as a dict"""
        return {field.name: field for field in self._fields}
        
    def add_field(
        self, 
        name: str, 
        type: 'typing.Any', 
        description: str, 
        default: 'typing.Any' = None,
    ):
        """ Add a field to the table interface
        
        Args:
            name: str
                The field's name
            type: Any
                The field's type 
            description: str
                Short, human-readable description of the data stored in this field
            default: same as type (optional)
                The field default value
        """
        self._fields.append(FieldDefinition(name, type, description, default))

    def _validate_data(self, row: dict, replace: dict = None):
        """ Helper function for validating input data and replacing missing fields.
        
        Args:
            row: dict
                The input data to be validated
            replace: dict
                Values to replace missing fields. Overwrites the individual fields' 
                default values.

        Returns:
            validated: dict
                The validated data, with missing fields replaced.            
        """
        validated = {k: v.default for k,v in self.fields_asdict.items()}
        if replace is not None:
            validated.update(replace)
    
        validated.update(row)

        for k,v in validated.items():
            fields = self.fields_asdict
            assert k in fields, f"Invalid field name `{k}`"

            f = fields[k]
            if v is None:
                assert f.default is not None, f"A value must be specified for `{k}`"

                validated[k] = f.default

            else:
                assert isinstance(v, f.type), f"Field `{k}` expects input of type `{f.type.__name__}` but input has type `{type(v).__name__}`"

        return validated             

    def add(self, row: dict):
        """ Add an entry to the table 
        
        Args:
            row: dict
                Input data in the form of a dict, where the keys are the field names
                and the values are the values to be added to the database.
        """
        self.backend.add(self._validate_data(row))

    def set(self, idx: int, row: dict):
        """ Modify an existing entry in the table 
        
        Args:
            idx: int
                Row index
            row: dict
                New data to replace the existing data
        """
        self.backend.set(idx, self._validate_data(row), self.get(idx))

    def filter(self):
        """ Search the table
        
        Returns:
            indices: list[int]
                The indices of the rows matching the search criteria
        """
        raise NotImplementedError(not_impl_err_msg(self.__class__.__name__, "filter"))

    def get(self, indices: int | list[int] = None, fields: str | list[str] = None):
        """ Retrieve data from the table
        
        Args:
            indices: int | list[int]
                The indices of the entries to be returned. If None, all entries in the table are returned.
            fields: str | list[str]
                The fields to be returned. If None, all fields are returned.

        Returns:
            : list[tuple]
                The data
        """
        raise NotImplementedError(not_impl_err_msg(self.__class__.__name__, "get"))

    def __iter__(self):
        """ Iterate through the table """
        raise NotImplementedError(not_impl_err_msg(self.__class__.__name__, "__iter__"))

    def __str__(self) -> str:
        """ Nicely formatted summary of the table definition

        Returns:
            res: str
                Table summary
        """
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


