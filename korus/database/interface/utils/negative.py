from dataclasses import dataclass
from datetime import datetime, timedelta
import pandas as pd


@dataclass
class MonoPeriod:
    """TODO: docstring"""

    file_id: int
    deployment_id: int
    channel: int
    filename: str
    file_start_utc: datetime
    start_utc: datetime
    end_utc: datetime = None

    @property
    def has_ended(self):
        return self.end_utc is not None

    def update(self, filename: str, file_start_utc: datetime) -> bool:
        pass


@dataclass
class StereoPeriod:
    """TODO: docstring"""

    mono_periods: dict[int, MonoPeriod]

    def update(
        self,
        file_id: int,
        deployment_id: int,
        channel: int,
        filename: str,
        file_start_utc: datetime,
    ) -> MonoPeriod:
        if channel in self.mono_periods:
            p = self.mono_periods[channel]
            p.update(filename, file_start_utc)
            if p.has_ended:
                return p

        else:
            self.mono_periods[channel] = MonoPeriod(
                file_id=file_id,
                deployment_id=deployment_id,
                channel=channel,
                filename=filename,
                file_start_utc=file_start_utc,
                start_utc=file_start_utc,
            )


def find_empty_periods(
    files: pd.DataFrame, annots: pd.DataFrame, max_file_gap: float = 0.1
):
    """Find time periods without annotations.

    Notes:
     - Expects all files to have known UTC start times.
     - Annotation UTC start times are derived by adding the within-file start time (`start_ms`) to the file UTC start time

    Args:
        files: pandas.DataFrame
            Table of audio files.
        annots: pandas.DataFrame
            Table of annotations.
        max_file_gap: float
            Maximum temporal gap between audiofiles in seconds.
            Inter-annotation gaps are allowed to span multiple audio files provided the temporal gap between the files is below this value.

    Returns:
    """
    # make copies so we don't modify the input args
    files = files.copy()
    annots = annots.copy()

    # add start and end times to annotation table
    files = files.reset_index().set_index("filename")
    annots["start_utc"] = annots.apply(
        lambda r: files.loc[r.filename].start_utc
        + timedelta(microseconds=r.start_ms * 1e3),
        axis=1,
    )
    annots["end_utc"] = annots.apply(
        lambda r: r.start_utc + timedelta(microseconds=r.duration_ms * 1e3), axis=1
    )

    # sort chronologically
    files.sort_values(by="start_utc", inplace=True)
    annots.sort_values(by="start_utc", inplace=True)

    # reindex
    files = files.reset_index().set_index(["deployment_id", "start_utc"])
    annots = annots.reset_index().annots.set_index(
        ["deployment_id", "channel", "start_utc"]
    )
