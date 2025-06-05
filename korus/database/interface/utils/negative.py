from dataclasses import dataclass
from datetime import datetime, timedelta
import pandas as pd
import numpy as np


@dataclass
class MonoTimePeriod:
    """TODO: docstring"""

    deployment_id: int
    file_id: int
    channel: int
    filename: str
    start_utc: datetime
    file_end_utc: datetime
    max_file_gap: float
    end_utc: datetime = None

    @property
    def has_ended(self):
        return self.end_utc is not None

    def file_gap(self, file_start_utc):
        if self.prev_file_end_utc:
            return (file_start_utc - self.file_end_utc).total_seconds()
        else:
            return np.inf

    def new_annotation(self, annot_start_utc, annot_end_utc):
        if annot_start_utc <= self.start_utc:
            self.start_utc = max(annot_end_utc, self.start_utc)

        else:
            self.end_utc = annot_start_utc

    def new_file(
        self,
        file_id: int,
        filename: str,
        file_start_utc: datetime,
        file_end_utc: datetime,
    ):
        # if the period has ended, do nothing
        if self.has_ended:
            return

        # if the period starts *after* the new file, update its file attributes
        if self.start_utc >= file_start_utc:
            self.file_id = file_id
            self.filename = filename
            self.file_end_utc = file_end_utc

        # if the temporal gap to the previous file exceeds the maximum allowed gap, update the period's `end_utc` attribute
        elif self.file_gap(file_start_utc) > self.max_file_gap:
            self.end_utc = self.file_end_utc


class StereoTimePeriod:
    def __init__(self, deployment_id: int, max_file_gap: float):
        self.deployment_id = deployment_id
        self.max_file_gap = max_file_gap
        self.mono_periods = dict()

    def new_annotation(
        self,
        channel,
        annot_start_utc,
        annot_end_utc,
    ):
        # update the current mono time-period
        self.mono_periods[channel].new_annotation(annot_start_utc, annot_end_utc)

        # start a new mono time-period, if the current one has ended
        p = self.mono_periods[channel]
        if p.has_ended:
            self.mono_periods[channel] = MonoTimePeriod(
                deployment_id=self.deployment_id,
                max_file_gap=self.max_file_gap,
                file_id=p.file_id,
                channel=channel,
                filename=p.filename,
                file_end_utc=p.file_end_utc,
                start_utc=annot_end_utc,
            )

        # return the updated mono time-period (*not* the new one, if a new one was created)
        return p

    def new_file(
        self,
        channel: int,
        file_id: int,
        filename: str,
        file_start_utc: datetime,
        file_end_utc: datetime,
    ) -> MonoTimePeriod:
        # update the current mono time-period
        if channel in self.mono_periods:
            self.mono_periods[channel].new_file(
                file_id, filename, file_start_utc, file_end_utc
            )

        # start a new mono time-period, if there is none or the current one has ended
        p = self.mono_periods.get(channel, None)
        if not p or p.has_ended:
            self.mono_periods[channel] = MonoTimePeriod(
                deployment_id=self.deployment_id,
                max_file_gap=self.max_file_gap,
                file_id=file_id,
                channel=channel,
                filename=filename,
                file_end_utc=file_end_utc,
                start_utc=file_start_utc,
            )

        # return the updated mono time-period (*not* the new one, if a new one was created)
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
    files = files.reset_index().set_index(["deployment_id", "start_utc", "end_utc"])
    annots = annots.reset_index().annots.set_index(
        ["deployment_id", "channel", "start_utc", "end_utc"]
    )

    periods = []

    # loop over deployments
    for deployment_id, files_deploy in files.groupby(level=0):
        annots_deploy = annots.loc[deployment_id]

        stereo_period = StereoTimePeriod(deployment_id, max_file_gap)

        # loop over files
        for (_, file_start_utc, file_end_utc), file_row in files_deploy.iterrows():

            # loop over channels
            for channel in file_row.channel:

                # update the current stereo period with the new file
                mono_period = stereo_period.new_file(
                    channel=channel,
                    file_id=file_row.file_id,
                    filename=file_row.filename,
                    file_start_utc=file_start_utc,
                    file_end_utc=file_end_utc,
                )

                # if the period has ended, save it to the list
                if mono_period.has_ended:
                    periods.append(mono_period)

                try:
                    # select annotations for current channel
                    annots_chan = annots_deploy.loc[channel]

                except KeyError:
                    # if there are none, proceed to the next channel
                    continue

                # select annotations that have start time within current file
                annots_file = annots_chan.loc[
                    (annots_chan.index >= file_start_utc)
                    & (annots_chan.index < file_end_utc)
                ]

                # loop over annotations
                for (start_utc, end_utc), _ in annots_file.iterrows():
                    mono_period = stereo_period.new_annotation(start_utc, end_utc)

                    # if the period has ended, save it to the list
                    if mono_period.has_ended:
                        periods.append(mono_period)
