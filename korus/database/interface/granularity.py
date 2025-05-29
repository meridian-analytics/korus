from .interface import TableInterface


class GranularityInterface(TableInterface):
    def __init__(self, backend):
        super().__init__("granularity", backend)

        self.add_field("name", str, "Granularity level name")
        self.add_field("description", str, "Definition of the granularity level")

        # add default granularity levels
        if len(self) == 0:
            self.add(
                {
                    "name": "unit",
                    "description": "Annotation of a single vocalisation/sound."
                    + " Bounding box drawn snuggly around a single vocalisation/sound."
                    + " Overlapping sounds may be present.",
                }
            )
            self.add(
                {
                    "name": "window",
                    "description": "Annotation of a single vocalisation/sound."
                    + " Box width does not necessarily match sound duration."
                    + " Sound may be fully or only partially within the box."
                    + " Overlapping sounds may be present.",
                }
            )
            self.add(
                {
                    "name": "file",
                    "description": "Annotation spanning precisely the duration of a single audio file.",
                }
            )
            self.add(
                {
                    "name": "batch",
                    "description": "Annotation of multiple vocalisations/sounds.",
                }
            )
            self.add(
                {
                    "name": "encounter",
                    "description": "Annotation of an entire (biological) acoustic encounter.",
                }
            )
