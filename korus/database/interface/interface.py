from dataclasses import dataclass, asdict
import textwrap
from tabulate import tabulate
from korus.database.backend import TableBackend
import numpy as np
import pandas as pd


@dataclass
class FieldDefinition:
    """Definition of a field in a Korus table interface.

    Fields must have one of the following Python types:

        bool,str,int,float,datetime

    Table indices are stored in fields with type `int` and name ending in `_id`.

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
        is_path: bool
            Whether the field is an OS path.

    Properties:
        is_index: bool
            Whether the field is a table index
    """

    name: str
    type: type
    description: str
    required: bool = True
    default: "typing.Any" = None
    options: list = None
    is_path: bool = False

    @property
    def is_index(self) -> bool:
        return self.type == int and len(self.name) > 2 and self.name[-3:] == "_id"

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

    def info(self) -> str:
        return (
            tabulate(
                [self.as_tuple_str()],
                headers=[
                    "Name",
                    "Type",
                    "Description",
                    "Required",
                    "Default Value",
                    "Allowed Values",
                ],
            )
            + "\n"
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
        self.indices = None
        self._load_fields()

    @property
    def fields(self) -> list[FieldDefinition]:
        """The definitions of the table's fields"""
        return self._fields

    @property
    def names(self) -> list[str]:
        """The names of the fields in the table"""
        return [field.name for field in self._fields]

    @property
    def field_names(self) -> list[str]:
        """The names of the fields in the table"""
        return self.names

    @property
    def fields_asdict(self) -> dict[str, FieldDefinition]:
        """The definitions of the table's fields as a dict"""
        return {field.name: field for field in self._fields}

    @property
    def aliases(self) -> list[FieldAlias]:
        """The field aliases"""
        return self._aliases

    @property
    def alias_names(self) -> list[str]:
        """The names of the table aliases"""
        return [alias.name for alias in self.aliases]

    @property
    def aliases_asdict(self) -> dict[str, FieldAlias]:
        """The field aliases as a dict"""
        return {alias.name: alias for alias in self._aliases}

    def field_name(self, name: str):
        """Given field or alias name, return field name"""
        if name in self.field_names:
            return name

        elif name in self.alias_names:
            return self.aliases[self.alias_names.index(name)].field_name

        else:
            raise ValueError(
                f"`{name}` does not match any field definitions or aliases"
            )

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
                If True, the index value is added to the dict with the key `id`.

        Returns:
            : dict
                The values in a dictionary with field names as keys.
        """
        if isinstance(fields, str):
            fields = [fields]

        elif fields is None:
            fields = self.names

        if index:
            fields = ["id"] + fields

        return {name: value for name, value in zip(fields, values)}

    def _create_field(
        self,
        name: str,
        type: "typing.Any",
        description: str,
        required: bool = True,
        default: "typing.Any" = None,
        options: list = None,
        is_path: bool = False,
    ):
        """Helper function for creating fields.

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
            is_path: bool
                Whether the field is an OS path.

        Returns:
            field: FieldDefinition
                The created field
        """
        field = FieldDefinition(
            name, type, description, required, default, options, is_path
        )
        self._fields.append(field)
        return field

    def _load_fields(self):
        """Helper function for loading custom fields from the database"""
        for field_attrs in self.backend.get_fields():
            self._create_field(**field_attrs)

    def add_field(
        self,
        name: str,
        type: "typing.Any",
        description: str,
        required: bool = True,
        default: "typing.Any" = None,
        options: list = None,
        is_path: bool = False,
    ):
        """Add a custom field to the table interface.

        The field is saved to the database. This allows it to be automatically re-created
        at every subsequent connection to the database.

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
            is_path: bool
                Whether the field is an OS path.
        """
        field = self._create_field(
            name, type, description, required, default, options, is_path
        )
        self.backend.save_field(asdict(field))

    def create_alias(
        self,
        field_name: str,
        name: str,
        type: "typing.Any",
        description: str,
        transform: callable = None,
        reverse_transform: callable = None,
    ):
        """Create a custom alias.

        OBS: Note that the alias is *not* saved to the database, so the alias needs to be re-created every time the table interface is instantiated.

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
        alias = FieldAlias(
            field_name, name, type, description, transform, reverse_transform
        )
        self._aliases.append(alias)

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

    def _apply_alias_transforms(self, row: dict, **kwargs) -> dict:
        """Helper function for transforming alias values to field values.
        Replaces values and renames keys in the input dictionary.
        """
        for alias in self._aliases:
            for append in ["", "~"]:
                if alias.name + append in row:
                    v = row.pop(alias.name + append)
                    _kwargs = kwargs.copy()
                    _kwargs.update(row)
                    row[alias.field_name + append] = alias.transform(v, **_kwargs)

        return row

    def _apply_reverse_alias_transforms(
        self, values: tuple, fields: str | tuple[str] = None, index: bool = False
    ) -> tuple:
        """Helper function for transforming field values to alias values.
        For lists and tuples, the transforms are applied to the individual elements.
        """
        row = self.values_asdict(values, fields, index)

        for name, value in row.items():
            if name in self.aliases_asdict:
                row[name] = self.aliases_asdict[name].reverse_transform(value, **row)

        values = [v for v in row.values()]
        return tuple(values)

    def add(self, row: dict) -> int:
        """Add an entry to the table

        Args:
            row: dict
                Input data in the form of a dict, where the keys are the field names
                and the values are the values to be added to the database.

        Returns:
            : int
                Index assigned to the added entry.
        """
        row = row.copy()
        row = self._apply_alias_transforms(row)
        row = self._replace_missing_values(row)
        row = self._validate_data(row)
        try:
            return self.backend.add(row)
        except Exception as err:
            err_msg = f"Attempt to add a new row to the {self.name} table failed due to an error in the backend."
            err.args = (err_msg,) + err.args
            raise

    def remove(self, indices: int | list[int] = None):
        """Remove entries from the table.

        Args:
            indices: int | list[int]
                The indices of the entries to be removed. If None, all entries will be removed.
        """
        self.backend.remove(indices)

    def update(self, idx: int, row: dict):
        """Modify an existing entry in the table

        Args:
            idx: int
                Row index
            row: dict
                New data to replace the existing data
        """
        row = row.copy()
        row = self._apply_alias_transforms(row)
        current_values = self.values_asdict(self.get(idx)[0])
        row = self._replace_missing_values(row, current_values)
        row = self._validate_data(row)
        try:
            self.backend.update(idx, row)
        except Exception as err:
            err_msg = f"Attempt to update row {idx} in the {self.name} table failed due to an error in the backend."
            err.args = (err_msg,) + err.args
            raise

    def get(
        self,
        indices: int | list[int] = None,
        fields: str | list[str] = None,
        return_indices: bool = False,
        always_tuple: bool = True,
        as_pandas: bool = False,
    ) -> list[tuple]:
        """Retrieve data from the table.

        Note that the method always returns a list, even when only a single index is specified.

        Args:
            indices: int | list[int]
                The indices of the entries to be returned. If None, all entries in the table are returned.
            fields: str | list[str]
                The fields to be returned. If None, all fields are returned. Can also be aliases.
            return_indices: bool
                Whether to also return the indices. If True, indices are inserted at the beginning of each row tuple.
            always_tuple: bool
                If False, and there is only 1 field, return a list of values instead of tuples.
            as_pandas: bool
                Return data in the form of a Pandas DataFrame.

        Returns:
            data: list[tuple]
                The data
        """
        if fields is None:
            fields = self.names

        if isinstance(fields, str):
            fields = [fields]

        # replace alias names with field names
        fields_noalias = [self.field_name(name) for name in fields]

        # unique indices
        if isinstance(indices, int):
            indices = [indices]

        if indices is None:
            unique = inverse = None
        else:
            unique, inverse = np.unique(indices, return_inverse=True)

        # pass query to backend
        data = self.backend.get(unique, fields_noalias, return_indices)

        # inverse index mapping
        if indices is not None:
            data = [data[i] for i in inverse]

        # apply reverse alias transforms
        data = [
            self._apply_reverse_alias_transforms(values, fields, return_indices)
            for values in data
        ]

        # optionally, drop tuple dimension
        if not as_pandas and not always_tuple and len(data) >= 1 and len(data[0]) == 1:
            data = [values[0] for values in data]

        # optionally, reformat output as Pandas DataFrame
        if as_pandas:
            data = _as_pandas_dataframe(data, fields, return_indices)

        return data

    def get_next(
        self,
        fields: str | list[str] = None,
        return_indices: bool = False,
        always_tuple: bool = True,
        as_pandas: bool = False,
    ):
        """Get the next row from the table"""
        idx = next(self.backend)
        res = self.get(idx, fields, return_indices, always_tuple, as_pandas)
        if not as_pandas:
            res = res[0]

        return res

    def unique(self, field: str) -> list:
        """Get the unique values of a given field.

        Args:
            field: str
                The field name

        Returns:
            values: list
                The non-null, unique values
        """
        values = self.get(fields=field, always_tuple=False)
        values = list(set(values))
        values = [v for v in values if v is not None]
        return values

    def __len__(self) -> int:
        """Number of rows in the table"""
        return len(self.backend)

    def __iter__(self):
        """Iterate through the table"""
        self.backend.reset_cursor()
        return self

    def __next__(self):
        """Get the next row from the table"""
        return self.get(next(self.backend))[0]

    def reset_filter(self):
        """Reset the search filter"""
        self.indices = None
        return self

    def filter(self, *conditions, **kwargs):
        """Search the table.

        If the backend's filtering method accepts additional keyword arguments,
        these can be passed as keyword arguments to this method.

        Args:
            conditions: sequence of dict
                Search criteria, where the keys are the field names and
                the values are the search values. Use tuples to search on
                a range of values and lists to search on multiple values.
                Append `~` to the field name to invert the search criteria,
                i.e., to exclude values or a range of values.
                Multiple dicts are joined by a logical OR.
                Within each dict, search criteria are joined by a logical AND.

        Returns:
            self: TableInterface
                A reference to this instance
        """
        # apply alias transforms and validation conditions
        conditions = [
            self._validate_condition(self._apply_alias_transforms(c, **kwargs))
            for c in conditions
        ]

        # pass to backend
        self.indices = self.backend.filter(*conditions, indices=self.indices, **kwargs)

        return self

    def _validate_condition(self, condition: dict) -> dict:
        """Helper function for validating filter conditions.

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

        for key, value in condition.items():
            negation = key[-1] == "~"
            name = key[:-1] if negation else key

            assert (
                name in self.field_names
            ), f"Invalid field name `{name}` in filter condition"

            # if scalar, recast as list
            if not isinstance(value, (list, tuple)):
                value = [value]

            if isinstance(value, list):
                # ensure unique values
                value = list(set(value))

            condition[key] = value

        return condition

    def info(self) -> str:
        """Nicely formatted summary of the table definition.

        Returns:
            res: str
                Table definition
        """
        # table name
        res = f"\nName: {self.name}"

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
                "Field",
                "Alias",
                "Type",
                "Description",
            ],
        )

        res += "\n"

        return res

    def __str__(self) -> str:
        return self.info()


class TableViewer:
    """Class for viewing table contents

    Args:
        table: TableInterface
            The table interface
        fields: str | list[str]
            Which fields to include
        nrows: int
            Number of rows printed per page
        max_char: int
            Column width no. characters
        transforms: dict[str, callable]
            Custom transformations applied to individual fields
    """

    def __init__(
        self,
        table: TableInterface,
        fields: str | list[str] = None,
        nrows: int = 20,
        max_char: int = 60,
        transforms: dict = None,
    ):
        self.table = table
        self.fields = fields
        self.nrows = nrows
        self.max_char = max_char
        self.counter = 0
        self.transforms = dict() if transforms is None else transforms
        self.table.backend.reset_cursor()

    def __next__(self):
        """Returns a nicely formatted view of the next `nrows` of the table"""
        if self.counter >= len(self.table):
            raise StopIteration

        df = []
        for _ in range(self.nrows):
            try:
                row = self.table.get_next(
                    self.fields, return_indices=True, as_pandas=True
                )
                df.append(row)
                self.counter += 1

            except StopIteration:
                break

        df = pd.concat(df)

        # apply custom transformations, if any
        for k, fcn in self.transforms.items():
            df[k] = df[k].apply(lambda x: fcn(x))

        # enforce max no. characters per line
        for idx, row in df.iterrows():
            for col in df.columns.values:
                v = row[col]

                # wrap text to desired column width
                if isinstance(v, str):
                    df.loc[idx, col] = textwrap.fill(v, self.max_char)

        if len(df) == 1:
            header = f"Showing entry no. {self.counter} of {len(self.table)} entries"
        else:
            header = f"Showing entries {self.counter - len(df) + 1}-{self.counter} of {len(self.table)} entries"

        contents = tabulate(df, headers="keys", tablefmt="psql")
        return "\n" + header + "\n" + contents + "\n"

    def __iter__(self):
        self.table.backend.reset_cursor()
        self.counter = 0
        return self


def _as_pandas_dataframe(
    data: list[tuple], columns: list[str], index: bool
) -> pd.DataFrame:
    """Helper function for converting retrieved data to a Pandas DataFrame"""
    if index:
        columns = ["id"] + columns

    df = pd.DataFrame(data, columns=columns)

    if index:
        df.set_index("id", inplace=True)
        df.index.name = "id"

    return df
