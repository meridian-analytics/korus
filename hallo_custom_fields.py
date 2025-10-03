"""
# === deployment

self._create_field("owner", str, "Data owner or source", required=False)
self._create_field(
    "location",
    str,
    "General geographic location of deployment (e.g. Salish Sea, BC, Canada)",
    required=False,
)
self._create_field(
    "hydrophone", str, "Hydrophone make and model", required=False
)
self._create_field("license", str, "Data license", required=False)
self._create_field("bits_per_sample", int, "Digital resolution", required=False)
self._create_field("sample_rate", int, "Sampling rate in Hz", required=False)
self._create_field("num_channels", int, "Hydrophone channels", required=False)
self._create_field(
    "sensitivity",
    float,
    "Frequency-integrated, digitial sensitivity",
    required=False,
)
self._create_field("comments", str, "Additional observations", required=False)


# === file

self._create_field("format", str, "Audio format", required=False)
self._create_field("codec", str, "Audio codec", required=False)


# === job

self._create_field(
    "configuration",
    dict,
    "Model configuration or instructions given to the annotator",
    required=False,
)
    self._create_field(
        "start_utc",
        datetime,
        "Start time of the annotation effort",
        required=False,
    )
    self._create_field(
        "by_human",
        bool,
        "True, if annotations were made/validated by a human",
        default=True,
    )
    self._create_field(
        "by_machine",
        bool,
        "True, if annotations were made by a machine",
        default=False,
    )
    self._create_field(
        "issues", list, "Issues or limitations with the annotations", required=False
    )
    self._create_field("comments", str, "Additional observations", required=False)


=== storage

self._create_field(
    "address", str, "URL address or physical location", required=False
)
self._create_field("description", str, "Brief description", required=False)

"""
