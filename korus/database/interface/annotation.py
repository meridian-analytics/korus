import datetime
from korus.database.backend import TableBackend
from .interface import TableInterface
from .taxonomy import TaxonomyInterface
from .job import JobInterface


class AnnotationInterface(TableInterface):
    """
    TODO: overwrite get() method to provide conversion to ketos/raven formats
    """

    def __init__(
        self,
        backend: TableBackend,
        taxonomy_interface: TaxonomyInterface,
        job_interface: JobInterface,
    ):

        super().__init__("annotation", backend)

        self.taxonomy_interface = taxonomy_interface
        self.job_interface = job_interface

        # fields
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

        # aliases
        alias_description = "Specify label tuples in place of label IDs"
        self.add_alias(
            "label_id", 
            "label", 
            tuple, 
            alias_description,
            self._get_label_id, 
            self._get_label
        )
        self.add_alias(
            "tentative_label_id",
            "tentative_label",
            tuple,
            alias_description,
            self._get_label_id,
            self._get_label,
        )
        self.add_alias(
            "ambiguous_label_id",
            "ambiguous_label",
            list,
            alias_description,
            self._get_label_id,
            self._get_label,
        )
        self.add_alias(
            "excluded_label_id",
            "excluded_label",
            list,
            alias_description,
            self._get_label_id,
            self._get_label,
        )

    def _get_label_id(self, label: tuple | list[tuple], job_id: int) -> int | list[int]:
        """Alias transform: convert labels to label IDs"""
        tax_version = self.job_interface.get(indices=job_id, fields="taxonomy_id")[0][0]
        label_id = self.taxonomy_interface.get_label_id(label, tax_version)
        return label_id

    def _get_label(self, label_id: int | list[int]) -> tuple | list[tuple]:
        """Reverse alias transform: convert label IDs to labels"""
        return self.taxonomy_interface.get_label(label_id)
