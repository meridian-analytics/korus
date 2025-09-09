import datetime
from .interface import TableInterface


class DeploymentInterface(TableInterface):
    def __init__(self, backend):
        super().__init__("deployment", backend)

        self._create_field("name", str, "Deployment name")
        self._create_field("owner", str, "Data owner or source", required=False)
        self._create_field(
            "start_utc", datetime.datetime, "Start time of deployment", required=False
        )
        self._create_field(
            "end_utc", datetime.datetime, "End time of deployment", required=False
        )
        self._create_field(
            "location",
            str,
            "General geographic location of deployment (e.g. Salish Sea, BC, Canada)",
            required=False,
        )
        self._create_field(
            "latitude_deg", float, "Deployment latitude in degrees", required=False
        )
        self._create_field(
            "longitude_deg", float, "Deployment longitude in degrees", required=False
        )
        self._create_field(
            "depth_m", float, "Deployment depth in meters", required=False
        )
        self._create_field(
            "trajectory",
            list,
            "Sequence of timestamped coordinates (timestamp,lat,lon,depth) [mobile deployments only]",
            required=False,
        )
        self._create_field(
            "latitude_min_deg",
            float,
            "Minimum latitude in degrees [mobile deployments only]",
            required=False,
        )
        self._create_field(
            "latitude_max_deg",
            float,
            "Maximum latitude in degrees [mobile deployments only]",
            required=False,
        )
        self._create_field(
            "longitude_min_deg",
            float,
            "Minimum longitude in degrees [mobile deployments only]",
            required=False,
        )
        self._create_field(
            "longitude_max_deg",
            float,
            "Maximum longitude in degrees [mobile deployments only]",
            required=False,
        )
        self._create_field(
            "depth_min_m",
            float,
            "Minimum depth in meters [mobile deployments only]",
            required=False,
        )
        self._create_field(
            "depth_max_m",
            float,
            "Maximum depth in meters [mobile deployments only]",
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
