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

    def link_file(self, file_id: int):
        #TODO: implement this method
        pass

    def file_table(self, job_id: int | list[int]):
        #TODO: implement this method
        """Returns a table of the audio files that were inspected as part of an annotation job or a set of jobs.

        The table has the following columns,

        * deployment_id (int): deployment index
        * job_id (int): job index
        * file_id (int): audio file index
        * filename (str): audio filename
        * relative_path (str): path to audio file relative to the base directory
        * base_path (str): absolute path to the base directory
        * sample_rate (int): sampling rate in samples/s
        * start_utc (datetime): file UTC start time
        * end_utc (datetime): file UTC end time
        * channel (str): the channel that was inspected (0;1;...)

        Args:
            job_id: int | list[int]
                Annotation job indices

        Returns:
            file_tbl: pandas.DataFrame
                File table
        """
        pass