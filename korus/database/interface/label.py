from .interface import TableInterface


class LabelInterface(TableInterface):
    """Defines the interface of the File Table."""

    def __init__(self, backend):
        super().__init__("label", backend)

        self.add_field("taxonomy_id", int, "Taxonomy index")
        self.add_field("sound_source_tag", str, "Sound source label", required=False)
        self.add_field("sound_source_id", str, "Sound source UUID", required=False)
        self.add_field("sound_type_tag", str, "Sound type label", required=False)
        self.add_field("sound_type_id", str, "Sound type UUID", required=False)
