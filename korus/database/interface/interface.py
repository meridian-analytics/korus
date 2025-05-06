from dataclasses import dataclass
from tabulate import tabulate
from korus.util import not_impl_err_msg


@dataclass
class FieldDefinition:
    """Definition of a field in a Korus table interface.

    Fields must have one of the following Python types:

        bool,str,int,float,datetime

    Attrs:
        name: str
            The name of the field
        type: type
            The field Python type.
        description: str
            Short, human-readable description of the data stored in this field
        required: bool
            True if the field is required to have a non-null value. False otherwise
        default: same as type (optional)
            The field default value
        options: list (optional)
            Allowed values
    """

    name: str
    type: type
    description: str
    required: bool = True
    default: "typing.Any" = None
    options: list = None

    def options_as_str(self) -> str:
        """Returns:
        : str
            Allowed values as a single, comma-separated string.
        """
        if self.options is None:
            return None

        else:
            return ", ".join([str(v) for v in self.options])

    def as_tuple_str(self) -> tuple:
        """Returns:
        : tuple
            The field definition in a tuple of strings.
        """
        return (
            self.name,
            self.type.__name__,
            self.description,
            "Y" if self.required else "N",
            str(self.default),
            self.options_as_str(),
        )


class TableInterface:
    """Base class for all Korus table interfaces.

    Table interfaces define how users interact with the database, e.g.,
    adding data to the database or retrieving data from it.

    Args:
        name: str
            The table interface name
        backend: korus.database.backend.TableBackend
            The table backend, connecting the interface to the underlying database
    """

    def __init__(self, name: str, backend):
        super().__init__()
        self.name = name
        self.backend = backend
        self._fields = []
        self._index = 0

    @property
    def fields(self) -> list[FieldDefinition]:
        """The definitions of the table's fields"""
        return self._fields

    @property
    def names(self) -> list[str]:
        """The names of the fields in the table"""
        return [field.name for field in self._fields]

    @property
    def fields_asdict(self) -> dict[str, FieldDefinition]:
        """The definitions of the table's fields as a dict"""
        return {field.name: field for field in self._fields}

    def values_asdict(self, values: tuple) -> dict:
        """A tuple of field values as a dict"""
        return {name: value for name, value in zip(self.names, values)}

    def add_field(
        self,
        name: str,
        type: "typing.Any",
        description: str,
        required: bool = True,
        default: "typing.Any" = None,
        options: list = None,
    ):
        """Add a field to the table interface.

        Args:
            name: str
                The field's name
            type: Any
                The field's type
            description: str
                Short, human-readable description of the data stored in this field
            required: bool
                True if the field is required to have a non-null value. False otherwise
            default: same as type (optional)
                The field default value
            options: list (optional)
                Allowed values
        """
        self._fields.append(
            FieldDefinition(name, type, description, required, default, options)
        )

    def _validate_data(self, row: dict, replace: dict = None):
        """Helper function for validating input data and replacing missing fields.

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
        # replace missing values with provided replacement values or default values
        validated = {k: v.default for k, v in self.fields_asdict.items()}
        if replace is not None:
            validated.update(replace)

        validated.update(row)

        # check for valid field names, valid types, valid values, and that required fields have non-null values
        for k, v in validated.items():
            fields = self.fields_asdict
            assert k in fields, f"Invalid field name `{k}`"

            f = fields[k]
            if v is None:
                assert not f.required, f"A value must be specified for `{k}`"

                validated[k] = v

            else:
                # check value
                if f.options is not None:
                    assert_msg = f"Invalid value `{v}` for field `{k}`. The only allowed values are: {f.options_as_str()}"
                    assert v in f.options, assert_msg

                # check type
                assert_msg = f"Field `{k}` expects input of type `{f.type.__name__}` but input has type `{type(v).__name__}`"
                assert isinstance(v, f.type), assert_msg

        return validated

    def add(self, row: dict):
        """Add an entry to the table

        Args:
            row: dict
                Input data in the form of a dict, where the keys are the field names
                and the values are the values to be added to the database.
        """
        self.backend.add(self._validate_data(row))

    def set(self, idx: int, row: dict):
        """Modify an existing entry in the table

        Args:
            idx: int
                Row index
            row: dict
                New data to replace the existing data
        """
        current_values = self.get(idx)[0]
        replace = {
            field.name: value for field, value in zip(self.fields, current_values)
        }
        self.backend.set(idx, self._validate_data(row, replace))

    def filter(self):
        """Search the table

        Returns:
            indices: list[int]
                The indices of the rows matching the search criteria
        """
        raise NotImplementedError(not_impl_err_msg(self.__class__.__name__, "filter"))

    def get(self, indices: int | list[int] = None, fields: str | list[str] = None):
        """Retrieve data from the table.

        Note that the method always returns a list, even when only a single index is specified.

        Args:
            indices: int | list[int]
                The indices of the entries to be returned. If None, all entries in the table are returned.
            fields: str | list[str]
                The fields to be returned. If None, all fields are returned.

        Returns:
            : list[tuple]
                The data
        """
        return self.backend.get(indices, fields)

    def __len__(self) -> int:
        """Number of rows in the table"""
        return len(self.backend)

    def __iter__(self):
        """Iterate through the table"""
        return self

    def __next__(self):
        """Get the next row from the table"""
        if self._index >= len(self):
            raise StopIteration

        row = self.get(self._index)[0]
        self._index += 1
        return row

    def __str__(self) -> str:
        """Nicely formatted summary of the table definition

        Returns:
            res: str
                Table summary
        """
        # table name and no. entries
        res = f"Name: {self.name}\nEntries: {len(self)}"

        # fields
        res += "\nFields:\n"
        res += tabulate(
            [f.as_tuple_str() for f in self.fields],
            headers=[
                "Name",
                "Type",
                "Description",
                "Required",
                "Default Value",
                "Allowed Values",
            ],
        )

        return res
