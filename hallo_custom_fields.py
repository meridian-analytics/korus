'''
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
'''