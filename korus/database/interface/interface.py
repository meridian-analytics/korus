from dataclasses import dataclass, fields, MISSING
from tabulate import tabulate


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
        default: same as type (optional)
            The field default value
        description: str
            Short, human-readable description of the data stored in this field
    """
    name: str 
    type: type
    default: 'typing.Any'
    description: str


def _create_field_definitions(row_type, descriptions):
    """ Helper function for creating field definitions"""
    definitions = []
    for field in fields(row_type):
        default = None if field.default == MISSING else field.default
        definitions.append(FieldDefinition(
            field.name, field.type, default, descriptions[field.name]
        ))

    return definitions


class TableInterface:
    """ Base class for all Korus table interfaces.
    
    Table interfaces define how users interact with the database, e.g., 
    adding data to the database or retrieving data from it.

    Args:
        name: str
            The table interface name
    """
    def __init__(self, name: str):
        self.name = name

    @property
    def fields(self) -> list[FieldDefinition]:
        """ The definitions of the table's fields"""
        raise NotImplementedError("Must be implemented in child class")

    def filter(self):
        """ Search the table"""
        raise NotImplementedError("Must be implemented in child class")

    def get(self):
        """ Retrieve data from the table"""
        raise NotImplementedError("Must be implemented in child class")

    def add(self):
        """ Add data to the table """
        raise NotImplementedError("Must be implemented in child class")

    def __iter__(self):
        """ Iterate through the table """
        raise NotImplementedError("Must be implemented in child class")

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


