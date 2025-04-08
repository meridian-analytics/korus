from dataclasses import dataclass, fields, MISSING
from tabulate import tabulate


@dataclass
class FieldDefinition:
    name: str 
    type: type
    default: 'typing.Any'
    description: str


def _create_field_definitions(row_type, descriptions):
    definitions = []
    for field in fields(row_type):
        default = None if field.default == MISSING else field.default
        definitions.append(FieldDefinition(
            field.name, field.type, default, descriptions[field.name]
        ))

    return definitions


class TableInterface:
    def __init__(self, name: str):
        self.name = name

    @property
    def fields(self) -> list[FieldDefinition]:
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
