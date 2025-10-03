import datetime
from .interface import TableInterface


class DeploymentInterface(TableInterface):
    def __init__(self, backend):
        super().__init__("deployment", backend)

        self._create_field("name", str, "Deployment name", required=True)
        self._create_field(
            "start_utc", datetime.datetime, "Start time of deployment", required=False
        )
        self._create_field(
            "end_utc", datetime.datetime, "End time of deployment", required=False
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
            "Sequence of timestamped coordinates (timestamp,lat,lon,depth)",
            required=False,
        )
