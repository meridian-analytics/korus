import datetime
from .interface import TableInterface


class AnnotationInterface(TableInterface):
    def __init__(self, backend):
        super().__init__("annotation", backend)

        self.add_field("deployment_id", int, "Deployment index")
        self.add_field("job_id", int, "Job index")
        self.add_field("file_id", int, "File index", required=False)
        self.add_field("label_id", int, "Label index", required=False)
        self.add_field(
            "tenative_label_id", int, "Tentative label index", required=False
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

    def get(self):
        """TODO: handle conversion to different formats: raven,ketos"""
        pass

    def filter(self, spatiotemporal_cut: callable):
        """TODO:

        spatiotemporal_cut:  fcn(t,lat,lon,z) -> bool
        """
        pass
