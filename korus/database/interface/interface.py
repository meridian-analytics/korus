from dataclasses import dataclass
from tabulate import tabulate
from korus.util import not_impl_err_msg


@dataclass
class FieldDefinition:
    """ Definition of a field in a Korus table interface.

    Fields must have one of the following Python types:

        bool,str,int,float, TODO: OTHERS?
    
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
        backend: 
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
        
    def add_field(self, name, type, description, default=None):
        self._fields.append(FieldDefinition(name, type, description, default))

    def _validate_data(self, row):
        validated = self.fields_asdict.copy()
        validated.update(row)

        for k,v in validated.items():
            fields = self.fields_asdict
            assert k in fields, f"Invalid field name `{k}`"

            f = fields[k]
            if v is None:
                assert f.default is None, f"A value must be specified for the field `{k}`"

                validated[k] = f.default

            assert isinstance(v, f.type), f"Field `{k}` expects input of type `{f.type.__name__}`"

        return validated             

    def add(self, row):
        """ Add data to the table """
        self.backend.add(self._validate_data(row))

    def set(self, idx, row):
        """ Modify an existing entry in the table """
        self.backend.set(idx, self._validate_data(row))

    def filter(self):
        """ Search the table"""
        raise NotImplementedError(not_impl_err_msg(self.__class__.__name__, "filter"))

    def get(self, indices=None, fields=None):
        """ Retrieve data from the table"""
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


