from dataclasses import dataclass
from tabulate import tabulate
from korus.database.backend import TableBackend


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


@dataclass
class FieldAlias:
    """Definition of a field alias in a Korus table interface.

    Attrs:
        field_name: str
            The field's name
        name: str
            The alias name
        type: Any
            The alias type
        description: str
            Short, human-readable description of the alias
        transform: callable (optional)
            Transform applied to every alias value to convert it to a field value
        reverse_transform: callable (optional)
            Transform applied to every field value to convert it to an alias value
    """

    field_name: str
    name: str
    type: type
    description: str
    transform: callable = lambda x, **_: x
    reverse_transform: callable = lambda x, **_: x

    def as_tuple_str(self) -> tuple:
        """Returns:
        : tuple
            The alias definition in a tuple of strings.
        """
        return (
            self.field_name,
            self.name,
            self.type.__name__,
            self.description,
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

    def __init__(self, name: str, backend: TableBackend):
        super().__init__()
        self.name = name
        self.backend = backend
        self._fields = []
        self._aliases = []
        self._index = -1
        self._count = 0

    @property
    def fields(self) -> list[FieldDefinition]:
        """The definitions of the table's fields"""
        return self._fields

    @property
    def aliases(self) -> list[FieldAlias]:
        """The field aliases"""
        return self._aliases

    @property
    def names(self) -> list[str]:
        """The names of the fields in the table"""
        return [field.name for field in self._fields]

    @property
    def fields_asdict(self) -> dict[str, FieldDefinition]:
        """The definitions of the table's fields as a dict"""
        return {field.name: field for field in self._fields}

    def values_asdict(
        self, values: tuple, fields: str | list[str] = None, index: bool = False
    ) -> dict:
        """Convert a tuple of field values to a dict, with field names as keys.

        Args:
            values: tuple
                The values
            fields: str | list[str]
                The field names. If None, the value tuple is assumed to contain values
                for every field, in the correct ordering.
            index: bool
                Whether the index has been inserted at the beginning of the value tuple.
        """
        if isinstance(fields, str):
            fields = [fields]

        elif fields is None:
            fields = self.names

        if index:
            fields = ["id"] + fields

        return {name: value for name, value in zip(fields, values)}

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

    def add_alias(
        self,
        field_name: str,
        name: str,
        type: "typing.Any",
        description: str,
        transform: callable = None,
        reverse_transform: callable = None,
    ):
        """Add an alias to the table interface.

        Args:
            field_name: str
                The field's name
            name: str
                The alias name
            type: Any
                The alias type
            transform: callable (optional)
                Transform applied to every row of input data to convert the alias value to its corresponding field value.
                Expects the alias value as the first positional argument, and accepts other field/alias values as keyword arguments.
                Note: The transform is also applied to the `condition` argument of the `filter` method.
            reverse_transform: callable (optional)
                Transform applied to every row of output data to convert the field value to its corresponding alias value.
                Expects the field value as the first positional argument, and accepts other field/alias values as keyword arguments.
        """
        self._aliases.append(
            FieldAlias(
                field_name, name, type, description, transform, reverse_transform
            )
        )

    def _validate_data(self, row: dict):
        """Helper function for validating input data.

        Args:
            row: dict
                The input data to be validated

        Returns:
            validated: dict
                The validated data
        """
        validated = row

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

    def _replace_missing_values(self, row: dict, replace: dict = None):
        """Helper function for replacing missing fields.

        Args:
            row: dict
                The input data to be validated
            replace: dict
                Values to replace missing fields. Overwrites the individual fields'
                default values.

        Returns:
            complete_row: dict
                The data, with missing fields replaced.
        """
        complete_row = {k: v.default for k, v in self.fields_asdict.items()}
        if replace is not None:
            complete_row.update(replace)

        complete_row.update(row)
        return complete_row

    def _apply_alias_transforms(self, row: dict) -> dict:
        """Helper function for transforming alias values to field values.
        Replaces values and renames keys in the input dictionary.
        """
        for alias in self._aliases:
            if alias.name in row:
                v = row[alias.name]
                row[alias.field_name] = alias.transform(v, **row)

        for alias in self._aliases:
            row.pop(alias.name, None)

        return row

    def _apply_reverse_alias_transforms(
        self, values: tuple, fields: str | tuple[str] = None, index: bool = False
    ) -> tuple:
        """Helper function for transforming field values to alias values.
        For lists and tuples, the transforms are applied to the individual elements.
        """
        row = self.values_asdict(values, fields, index)

        for alias in self._aliases:
            if alias.field_name in row:
                v = row[alias.field_name]
                row[alias.name] = alias.reverse_transform(v, **row)

        for alias in self._aliases:
            row.pop(alias.field_name, None)

        return tuple(row.values())

    def add(self, row: dict):
        """Add an entry to the table

        Args:
            row: dict
                Input data in the form of a dict, where the keys are the field names
                and the values are the values to be added to the database.
        """
        row = self._apply_alias_transforms(row)
        row = self._replace_missing_values(row)
        row = self._validate_data(row)
        self.backend.add(row)

    def set(self, idx: int, row: dict):
        """Modify an existing entry in the table

        Args:
            idx: int
                Row index
            row: dict
                New data to replace the existing data
        """
        row = self._apply_alias_transforms(row)
        current_values = self.values_asdict(self.get(idx, alias=False)[0])
        row = self._replace_missing_values(row, current_values)
        row = self._validate_data(row)
        self.backend.set(idx, row)

    def get(
        self,
        indices: int | list[int] = None,
        fields: str | list[str] = None,
        return_indices: bool = False,
        alias: bool = True,
    ) -> list[tuple]:
        """Retrieve data from the table.

        Note that the method always returns a list, even when only a single index is specified.

        Args:
            indices: int | list[int]
                The indices of the entries to be returned. If None, all entries in the table are returned.
            fields: str | list[str]
                The fields to be returned. If None, all fields are returned.
            return_indices: bool
                Whether to also return the indices. If True, indices are inserted at the beginning of each row tuple.
            alias: bool
                Whether to apply reverse alias transforms to replace field values by their alias values.

        Returns:
            data: list[tuple]
                The data
        """
        data = self.backend.get(indices, fields, return_indices)
        if alias:
            data = [
                self._apply_reverse_alias_transforms(values, fields, return_indices)
                for values in data
            ]

        return data

    def __len__(self) -> int:
        """Number of rows in the table"""
        return len(self.backend)

    def __iter__(self):
        """Iterate through the table"""
        self._count = 0
        self._index = -1
        return self

    def __next__(self):
        """Get the next row from the table"""
        self._count += 1

        if self._count > len(self):
            raise StopIteration

        while True:
            self._index += 1
            rows = self.get(self._index)
            if len(rows) > 0:
                return rows[0]

    def reset_filter(self):
        """Reset the search filter"""
        self.indices = None

    def filter(self, condition: dict = None, invert: bool = False):
        """Search the table.

        Args:
            condition: dict
                Search criteria, where the keys are the field names and
                the values are the search values. Use tuples to search on
                a range of values and lists to search on multiple values.
            invert: bool
                Invert the search, i.e., exclude values or a range of values.

        Returns:
            self: TableInterface
                A reference to this instance
        """
        condition = self._apply_alias_transforms(condition)
        condition = self._validate_condition(condition)
        self.indices = self.backend.filter(condition, invert, self.indices)
        return self

    def _validate_condition(self, condition: dict) -> dict:
        """Helper function for validating filter conditions.

        TODO: implement this method

        Args:
            condition: dict
                Search criteria

        Returns:
            condition: dict
                The validated criteria

        Raises:
            ValueError: if the validation fails
        """
        if condition is None:
            condition = dict()

        return condition

    def __str__(self) -> str:
        """Nicely formatted summary of the table definition

        TODO: also print summary of aliases

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

        if len(self._aliases) == 0:
            return res

        # aliases
        res += "\nAliases:\n"
        res += tabulate(
            [f.as_tuple_str() for f in self.aliases],
            headers=[
                "Field" "Alias",
                "Type",
                "Description",
            ],
        )

        return res
