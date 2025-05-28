import datetime
from korus.database.backend import TableBackend
from .interface import TableInterface
from .taxonomy import TaxonomyInterface


class AnnotationInterface(TableInterface):
    """
    Label fields are subject to conversion:

        * label
        * tentative_label
        * ambiguous_label
        * excluded_label

    """
    def __init__(self, backend: TableBackend, taxonomy_interface: TaxonomyInterface):
        super().__init__("annotation", backend)

        self.taxonomy_interface = taxonomy_interface

        self.add_field("deployment_id", int, "Deployment index")
        self.add_field("job_id", int, "Job index")
        self.add_field("file_id", int, "File index", required=False)
        self.add_field("label_id", int, "Label index", required=False)
        self.add_field(
            "tentative_label_id", int, "Tentative label index", required=False
        )
        self.add_field(
            "ambiguous_label_id", list, "Ambiguous label indices", required=False
        )
        self.add_field(
            "excluded_label_id", list, "Excluded label indices", required=False
        )
        self.add_field("tag_id", list, "Tag indices", required=False)
        self.add_field("granularity_id", int, "Granularity index", default=1)
        self.add_field("num_files", int, "Number of audio files", default=1)
        self.add_field("file_id_list", list, "File indices", required=False)
        self.add_field("start_utc", datetime.datetime, "UTC start time", required=False)
        self.add_field("duration_ms", int, "Duration in milliseconds", required=False)
        self.add_field(
            "start_ms",
            int,
            "Start time in milliseconds from the beginning of the file",
            required=False,
        )
        self.add_field(
            "freq_min_hz", int, "Lower frequency bound in Hz", required=False
        )
        self.add_field(
            "freq_max_hz", int, "Upper frequency bound in Hz", required=False
        )
        self.add_field("channel", int, "Hydrophone channel", default=0)
        self.add_field("machine_prediction", dict, "Machine prediction", required=False)
        self.add_field("valid", bool, "Validation status", default=True)
        self.add_field("comments", str, "Additional observations", required=False)

    def _label_to_id(self, row):
        return row
    
    def _id_to_label(self, row):
        return row

    def add(self, row: dict):
        """Add an entry to the table

        Args:
            row: dict
                Input data in the form of a dict, where the keys are the field names
                and the values are the values to be added to the database.
        """
        row = self._validate_data(row)
        row = self._label_to_id(row)
        self.backend.add(row)

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
        replace = self._validate_data(row, replace)
        replace = self._label_to_id(replace)
        self.backend.set(idx, replace)

    def get(
        self,
        indices: int | list[int] = None,
        fields: str | list[str] = None,
        return_indices: bool = False,
    ) -> list[tuple]:
        """Retrieve data from the table.

        Note that the method always returns a list, even when only a single index is specified.

        TODO: handle conversion to different formats: raven,ketos

        Args:
            indices: int | list[int]
                The indices of the entries to be returned. If None, all entries in the table are returned.
            fields: str | list[str]
                The fields to be returned. If None, all fields are returned.

        Returns:
            : list[tuple]
                The data
        """
        res = self.backend.get(indices, fields, return_indices)
        return [self._id_to_label(row) for row in rows]

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
        condition = self._validate_condition(condition)
        condition = self._label_to_id(condition)
        self.indices = self.backend.filter(condition, invert, self.indices)
        return self