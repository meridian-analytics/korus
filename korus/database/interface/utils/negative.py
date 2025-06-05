from dataclasses import dataclass
from datetime import datetime, timedelta
import pandas as pd
import numpy as np


@dataclass
class MonoTimePeriod:
    """TODO: docstring"""

    file_id: int
    deployment_id: int
    channel: int
    filename: str
    file_start_utc: datetime
    start_utc: datetime
    max_file_gap: float
    prev_file_end_utc: datetime = None
    end_utc: datetime = None

    @property
    def has_ended(self):
        return self.end_utc is not None

    def file_gap(self, file_start_utc):
        if self.prev_file_end_utc:
            return (file_start_utc - self.prev_file_end_utc).total_seconds()
        else:
            return np.inf

    def update(self, filename: str, file_start_utc: datetime) -> bool:
        # if periods starts after file, update `file_start_utc` and `filename` attributes
        if self.start_utc >= file_start_utc:
            self.filename = filename
            self.file_start_utc = file_start_utc

        # if temporal gap to previous file exceeds the maximum allowed gap, 
        # end the time period by setting its `end_utc` attribute
        if self.file_gap(file_start_utc) > self.max_file_gap:
            self.end_utc = self.prev_file_end_utc


class StereoTimePeriod:
    def __init__(self, max_file_gap):
        self.max_file_gap = max_file_gap
        self.mono_periods = dict()

    def update(
        self,
        file_id: int,
        deployment_id: int,
        channel: int,
        filename: str,
        file_start_utc: datetime,
    ) -> MonoTimePeriod:
        # update the current mono time-period
        p = self.mono_periods.get(channel, None)
        if p and not p.has_ended:
            self.mono_periods[channel].update(filename, file_start_utc)

        # start a new mono time-period, if there is none or the current one has ended
        p = self.mono_periods.get(channel, None)
        if not p or p.has_ended:
            self.mono_periods[channel] = MonoTimePeriod(
                file_id=file_id,
                deployment_id=deployment_id,
                channel=channel,
                filename=filename,
                file_start_utc=file_start_utc,
                start_utc=file_start_utc,
                max_file_gap=self.max_file_gap,
            )

        # return the current mono time-period (*not* the new one, if a new one was created)
        return p

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

    periods = []

    # loop over deployments
    for deploy_id, files_deploy in files.groupby(level=0):
        annots_deploy = annots.loc[deploy_id]

        stereo_period = StereoTimePeriod(max_file_gap)

        # loop over files
        for (_, file_start_utc), file_row in files_deploy.iterrows():

            # loop over channels
            for channel in file_row.channel:

                mono_period = stereo_period.update(
                    file_id = file_row.file_id,
                    deployment_id = deploy_id,
                    channel = channel,
                    filename = file_row.filename,
                    file_start_utc = file_start_utc,                    
                )

                if mono_period.has_ended:
                    periods.append(mono_period)