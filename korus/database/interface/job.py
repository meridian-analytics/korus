import datetime
from .interface import TableInterface


class JobInterface(TableInterface):
    def __init__(self, backend):
        super().__init__("job", backend)

        self.add_field("taxonomy_id", int, "Taxonomy index", required=False)
        self.add_field("model_id", int, "Model index", required=False)
        self.add_field(
            "annotator",
            str,
            "Name of person or model who annotated the data",
            required=False,
        )
        self.add_field(
            "primary_sound", list, "Systematically annotated sounds", required=False
        )
        self.add_field(
            "background_sound",
            list,
            "Opportunistially annotated sounds",
            required=False,
        )
        self.add_field(
            "is_exhaustive",
            bool,
            "Whether all primary sounds were annotated",
            required=False,
        )
        self.add_field(
            "configuration",
            dict,
            "Model configuration or instructions given to the annotator",
            required=False,
        )
        self.add_field(
            "start_utc",
            datetime.datetime,
            "Start time of the annotation effort",
            required=False,
        )
        self.add_field(
            "end_utc",
            datetime.datetime,
            "End time of the annotation effort",
            required=False,
        )
        self.add_field(
            "by_human",
            bool,
            "True, if annotations were made/validated by a human",
            default=True,
        )
        self.add_field(
            "by_machine",
            bool,
            "True, if annotations were made by a machine",
            default=False,
        )
        self.add_field(
            "issues", list, "Issues or limitations with the annotations", required=False
        )
        self.add_field("comments", str, "Additional observations", required=False)

    def add_file(self, job_id: int, file_id: int, channel: int = 0):
        """Add an audio file to an annotation job

        Args:
            job_id: int
                The job index
            file_id: int
                The audiofile index
            channe: int
                Stereo channel, 0,1,...
        """
        self.backend.add_file(job_id, file_id, channel)

    def get_files(self, job_id: int | list[int]) -> list[int]:
        """Retrieve the IDs of all the files associated with an annotation job or multiple jobs.

        Args:
            job_id: int | list[int]
                The job indices

        Returns:
            : list[int]
                The file IDs.
        """
        return self.backend.get_file(job_id)
