from tqdm import tqdm
import pandas as pd
from datetime import datetime
from korus.database.backend import TableBackend
from .interface import TableInterface
from .taxonomy import TaxonomyInterface
from .job import JobInterface
from .file import FileInterface
from .tag import TagInterface
from .granularity import GranularityInterface
from .utils.negative import find_unannotated_periods
from .utils.selection import (
    compute_number_of_views,
    compute_view_centers,
    map_to_audiofile,
)
from .utils.validate import (
    validate_deployment_id,
    validate_duration,
    validate_file_id,
    validate_timestamps,
    validate_frequency,
)


def _id_from_name(interface: TableInterface, name: str | list[str]) -> list[int]:
    """Helper function for mapping tag/granularity names to indices.
    Raises ValueError if an invalid name is specified.
    """
    if name is None:
        return None

    names = name if isinstance(name, list) else [name]
    indices = []
    for name in names:
        idx = interface.reset_filter().filter({"name": name}).indices
        if len(idx) == 0:
            raise ValueError(f"Unrecognized {interface.name}: {name}")

        indices += idx

    return indices


class AnnotationInterface(TableInterface):
    def __init__(
        self,
        backend: TableBackend,
        taxonomy: TaxonomyInterface,
        job: JobInterface,
        file: FileInterface,
        tag: TagInterface,
        granularity: GranularityInterface,
    ):

        super().__init__("annotation", backend)

        # linked interfaces
        self._taxonomy = taxonomy
        self._job = job
        self._file = file
        self._tag = tag
        self._granularity = granularity

        # fields
        self.add_field("deployment_id", int, "Deployment index")
        self.add_field("job_id", int, "Job index")
        self.add_field("file_id", int, "File index", required=False)
        self.add_field(
            "label_id", int, "Label index for confident classification", required=False
        )
        self.add_field(
            "tentative_label_id",
            int,
            "Label index for tentative classification",
            required=False,
        )
        self.add_field(
            "ambiguous_label_id",
            list,
            "Label indices for ambiguous classification",
            required=False,
        )
        self.add_field(
            "excluded_label_id",
            list,
            "Label indices for excluded classes",
            required=False,
        )
        self.add_field(
            "multiple_label_id",
            list,
            "Label indices for multiple (batch) classification",
            required=False,
        )
        self.add_field("tag_id", list, "Tag indices", required=False)
        self.add_field("granularity_id", int, "Granularity index", default=1)
        self.add_field(
            "negative", bool, "Automatically generated negative", default=False
        )
        self.add_field("num_files", int, "Number of audio files", default=1)
        self.add_field("file_id_list", list, "File indices", required=False)
        self.add_field("start_utc", datetime, "UTC start time", required=False)
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

        # time aliases
        self.add_alias(
            "start_ms",
            "start",
            float,
            "Start time in seconds from the beginning of the file",
            lambda x, **_: int(x * 1e3),
            lambda x, **_: float(x) / 1e3,
        )

        self.add_alias(
            "duration_ms",
            "duration",
            float,
            "Duration in seconds",
            lambda x, **_: int(x * 1e3),
            lambda x, **_: float(x) / 1e3,
        )

        # tag and granularity aliases
        self.add_alias(
            "tag_id",
            "tag",
            list,
            "Tag name",
            self._get_tag_id,
            self._get_tag,
        )

        self.add_alias(
            "granularity_id",
            "granularity",
            str,
            "Granularity level",
            self._get_granularity_id,
            self._get_granularity,
        )

        # label aliases
        alias_description = "Specify label tuples in place of label IDs"
        self.add_alias(
            "label_id",
            "label",
            tuple,
            alias_description,
            self._get_label_id,
            self._get_label,
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
        self.add_alias(
            "multiple_label_id",
            "multiple_label",
            list,
            alias_description,
            self._get_label_id,
            self._get_label,
        )

    def _get_label_id(self, label: tuple | list[tuple], **kwargs) -> int | list[int]:
        """Alias transform: convert label to label ID"""
        if "job_id" in kwargs:
            tax_version = self._job.get(kwargs["job_id"], "taxonomy_id")[0][0]
        else:
            tax_version = kwargs.get("taxonomy_version", None)

        label_id = self._taxonomy.get_label_id(label, tax_version)
        return label_id

    def _get_label(self, label_id: int | list[int], **_) -> tuple | list[tuple]:
        """Reverse alias transform: convert label ID to label"""
        return self._taxonomy.get_label(label_id)

    def _get_tag_id(self, name: str | list[str], **kwargs) -> list[int]:
        """Alias transform: convert tag to tag ID"""
        return _id_from_name(self._tag, name)

    def _get_tag(self, id: int | list[int], **kwargs) -> str | list[str]:
        """Reverse alias transform: convert tag ID to tag"""
        if id is None:
            return None
        else:
            return self._tag.get(id, "name", always_tuple=False)

    def _get_granularity_id(self, name: str | list[str], **kwargs) -> int | list[int]:
        """Alias transform: convert granularity to granularity ID"""
        indices = _id_from_name(self._granularity, name)
        return indices if isinstance(name, list) else indices[0]

    def _get_granularity(self, id: int | list[int], **kwargs) -> str | list[str]:
        """Reverse alias transform: convert granularity ID to granularity"""
        values = self._granularity.get(id, "name", always_tuple=False)
        return values if isinstance(id, list) else values[0]

    def add(self, row: dict):
        """Add an entry to the table.

        If the deployment ID is not specified, it will be inferred from file ID.

        Either the UTC start time of the annotation or the within-file start time must be specified.

        If the UTC start time is not specified, it will be inferred from the within-file start time, using the audio file's UTC start time.
        Conversely, if the within-file start time is not specified, it will be inferred from the UTC start time.

        If the duration is not specified, it is inferred assuming that the annotation extends to the end
        of the specified audiofile(s).

        Args:
            row: dict
                Input data in the form of a dict, where the keys are the field names
                and the values are the values to be added to the database.
        """
        row = row.copy()
        row = self._apply_alias_transforms(row)

        # validate/infer deployment ID
        row = validate_deployment_id(row, self._file)

        # validate/infer start time
        row = validate_timestamps(row, self._file)

        # validate/infer file IDs
        row = validate_file_id(row, self._file)

        # validate/infer duration
        row = validate_duration(row, self._file)

        # validate/infer frequency limits
        row = validate_frequency(row, self._file)

        super().add(row)

    def generate_negatives(self, job_id: int):
        """Generate negative annotations.

        Here, negatives are understood as (uninterrupted) time periods during which no sounds were annotated.

        Negatives are added to the annotation table with `negative=True`.

        Args:
            job_id: int
                Job index
        """
        # get file data
        files = self._job.get_filedata(job_id)

        # if there are no files, do nothing
        if len(files) == 0:
            return

        # remove all pre-existing negatives for this job
        indices = self.reset_filter().filter(job_id=job_id, negative=True).indices
        self.remove(indices)
        self.reset_filter()

        # get annotation data
        annots = self.get(
            fields=["deployment_id", "file_id", "channel", "start", "duration"],
            as_pandas=True,
        )

        # find time periods without annotations
        negatives = find_unannotated_periods(files, annots)

        # mark as 'negative' and set job_id
        negatives["negative"] = True
        negatives["job_id"] = job_id

        # target sounds for this job
        target = self._job.get(job_id, fields="target", always_tuple=False)[0]

        # set excluded_label
        if target is not None:
            negatives["excluded_label"] = [target for _ in range(len(negatives))]

        # add new negatives to table
        for _, row in negatives.iterrows():
            self.add(row.to_dict())

    def validate_raven(
        self,
    ):
        raise NotImplementedError()

    def from_raven(
        self,
    ):
        raise NotImplementedError()

    def to_raven(
        self,
    ):
        raise NotImplementedError()

    def create_selections(
        self,
        indices: list[int],
        window: float,
        step: float = None,
        center: bool = False,
        exclusive: bool = False,
        num_max: int = None,
        exclude: tuple[str, str] | list[tuple[str, str]] = None,
        data_support: bool = True,
        progress_bar: bool = False,
        full_path: str = True,
    ):
        """Create uniform-length selection windows on a set of annotations.

        Args:
            indices: list[int]
                Annotation indices
            window: float
                Window size in seconds.
            step: float
                Step size in seconds. Used for creating temporally translated views of the same annotation.
                If None, at most one (1) selection will be created per annotation.
            center: bool
                Align the selection window temporally with the midpoint of the annotation. If False, the temporal
                alignment will be chosen at random (uniform distribution).
            exclusive: bool
                If True, the selection window is not allowed to contain anything but the annotated section of data.
                In other words, the selection window is not allowed extend beyond the start/end point of the annotation.
                In particular, this means that selections will not be created for annotations shorther than @window_ms.
                Default is False.
            num_max: int
                Create at most this many selections.
            exclude: tuple[str, str] | list[tuple[str, str]]
                Only return selections that have been verified to not contain sounds with this (source,type) label.
                Note that the requirement extends to all ancestral and descendant nodes in the taxonomy tree.
                NOT YET IMPLEMENTED.
            data_support: bool
                If True, selection windows are not allowed to extend beyond the start/end times of the audio files in the database.
                Default is True.
            progress_bar: bool
                Whether to display a progress bar. Default is False.
            full_path: bool
                Whether to include the full audio file paths in the output table. Default is True.

        Returns:
            : Pandas DataFrame
                Selection table
        """
        if exclude:
            raise NotImplementedError(
                "Creation of selections with `exclude` argument is not implemented"
            )

        # get annotation data
        annots = self.get(
            indices=indices,
            fields=[
                "deployment_id",
                "file_id",
                "channel",
                "start_utc",
                "start",
                "duration",
            ],
            return_index=True,
            as_pandas=True,
        )
        annots = annots.rename(columns={"id": "annot_id"})

        # if exclusive=True, discard all annotations shorter than @window_ms
        if exclusive:
            annots = annots[annots.duration >= window]

        # compute no. views of each annotation
        annots = compute_number_of_views(annots, window, num_max, step is not None)

        # discard annotations with 0 views
        annots = annots[annots.num_view != 0]

        # get file data
        files = self._file.get(
            indices=annots.file_id.unique(),
            fields=["start_utc"],
            return_index=True,
            as_pandas=True,
        )
        files = files.set_index("id")
        files["duration"] = self._file.get_duration(indices)
        files["absolute_path"] = self._file.get_absolute_path(indices)

        # loop over annotations
        selections = []
        for idx, row in tqdm(
            annots.iterrows(), total=annots.shape[0], disable=not progress_bar
        ):

            # compute center UTC times of the views
            view_centers = compute_view_centers(row, window, step, center)

            # map times to audio files
            views = map_to_audiofile(
                row, window, view_centers, files, full_path, data_support
            )

            if len(views) == 0:
                continue

            # assign IDs to the selections
            views.sel_id += len(selections)

            # append the annotation index
            views["annot_id"] = row.annot_id

            # collect selections and increment counter
            selections.append(views)

        # concatenate into a pandas DataFrame, set index and round to appropriate digits
        selections = (
            pd.concat(selections, ignore_index=True)
            .set_index(["sel_id", "filename"])
            .round({"start": 3, "end": 3})
        )

        return selections

    def filter(self, *conditions: dict, **kwargs):
        """Search the table.

        Note: Search criteria specified by keyword arguments take priority over search criteria
        specified using the positional arguments. Specifically, keyword search criteria are
        inserted into every condition dict replacing any pre-existing criteria for the same field.

        Args:
            conditions: sequence of dict
                Search criteria, where the keys are the field names and
                the values are the search values. Use tuples to search on
                a range of values and lists to search on multiple values.

        Keyword args:
            select: tuple | list[tuple]
                Select annotations with this (source,type) label.
                The character '*' can be used as wildcard.
                Accepts both a single tuple and a list of tuples.
                By default all descendant nodes in the taxonomy tree are also considered. Use
                the `strict` argument to change this behaviour.
            exclude: tuple | list[tuple]
                Exclude annotations with this (source,type) label, but select annotations with
                this (source,type) *excluded_label*.
                The character '*' can be used as wildcard.
                Accepts both a single tuple and a list of tuples.
                By default all descendant nodes in the taxonomy tree are also considered. Use
                the `strict` argument to change this behaviour.
            strict: bool
                Whether to interpret labels 'strictly', meaning that ancestral/descendant nodes
                in the taxonomy tree are not considered. For example, when filtering on 'KW'
                annotations labelled as 'SRKW' will *not* be selected if `strict` is set to True.
                Default is False. NOT YET IMPLEMENTED.
            tentative: bool
                Whether to filter on tentative label assignments, when available. Default is False.
            ambiguous: bool
                Whether to also filter on ambiguous label assignments. Default is False.
            file: bool
                If True, only include annotations pertaining to audio files in the database.
                Default is False. NOT YET IMPLEMENTED.
            taxonomy_version: int
                Acoustic taxonomy that the (source,type) label arguments refer to. If not specified,
                the latest version will be used.

        Returns:
            self: TableInterface
                A reference to this instance
        """
        if kwargs.pop("strict", False):
            raise NotImplementedError("`strict` filter condition not yet implemented")

        if kwargs.pop("file", False):
            raise NotImplementedError("`file` filter condition not yet implemented")

        conditions = list(conditions).copy() if len(conditions) > 0 else [dict()]

        # perform `select` search
        select_conditions = []
        for cond in conditions:
            select_conditions += self._create_select_condition(cond, **kwargs)

        super().filter(*select_conditions, **kwargs)

        # perform `exclude` search
        if "exclude" in kwargs:
            exclude_conditions = []
            for cond in conditions:
                exclude_conditions += self._create_exclude_condition(cond, **kwargs)

            super().filter(*exclude_conditions, **kwargs)

        return self

    def _create_select_condition(self, condition: dict, **kwargs) -> list[dict]:
        """Helper function for inserting `select` search criteria into condition dict"""
        select = kwargs.get("select", None)
        tentative = kwargs.get("tentative", False)
        ambiguous = kwargs.get("ambiguous", False)
        tax_version = kwargs.get("taxonomy_version", None)

        # if keyword arg matches field or alias name, insert into dict, overwriting existing field if present
        for k, v in kwargs.items():
            if k in self.field_names + self.alias_names:
                condition[k] = v

        conds = [condition.copy()]

        # selected labels
        if select is not None:

            id = self._taxonomy.get_label_id(select, tax_version)

            # crosswalk labels to all taxonomies, including descendant nodes
            id = self._taxonomy.crosswalk(id, descend=True, equivalent_only=True)

            # confident
            conds[len(conds) - 1]["label_id"] = id

            # multiple
            conds.append(condition.copy())
            conds[len(conds) - 1]["multiple_label_id"] = id

            # tentative
            if tentative:
                conds.append(condition.copy())
                conds[len(conds) - 1]["tentative_label_id"] = id

            # ambiguous
            if ambiguous:
                conds.append(condition.copy())
                conds[len(conds) - 1]["ambiguous_label_id"] = id

        return conds

    def _create_exclude_condition(self, condition: dict, **kwargs) -> list[dict]:
        """Helper function for inserting `exclude` search criteria into condition dict"""
        exclude = kwargs.get("exclude", None)
        tentative = kwargs.get("tentative", False)
        ambiguous = kwargs.get("ambiguous", False)
        tax_version = kwargs.get("taxonomy_version", None)

        # if keyword arg matches field or alias name, insert into dict, overwriting existing field if present
        for k, v in kwargs.items():
            if k in self.field_names + self.alias_names:
                condition[k] = v

        id = self._taxonomy.get_label_id(exclude, tax_version)

        # crosswalk labels to all taxonomies, including only ascendant nodes and not requiring equivalency
        exclude_id = self._taxonomy.crosswalk(id, ascend=True, equivalent_only=False)

        # crosswalk labels to all taxonomies, including ascendant and descendant nodes
        select_id = self._taxonomy.crosswalk(
            id, ascend=True, descend=True, equivalent_only=True
        )

        conds = []

        # exclude
        conds.append(condition.copy())
        conds[len(conds) - 1]["excluded_label_id"] = exclude_id

        # confident
        conds.append(condition.copy())
        conds[len(conds) - 1]["label_id~"] = select_id

        # multiple
        conds.append(condition.copy())
        conds[len(conds) - 1]["multiple_label_id~"] = select_id

        # tentative
        if tentative:
            conds.append(condition.copy())
            conds[len(conds) - 1]["tentative_label_id~"] = select_id

        # ambiguous
        if ambiguous:
            conds.append(condition.copy())
            conds[len(conds) - 1]["ambiguous_label_id~"] = select_id

        return conds
