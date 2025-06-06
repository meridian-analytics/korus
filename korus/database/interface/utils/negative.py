from dataclasses import dataclass
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from copy import deepcopy


@dataclass
class MonoTimePeriod:
    """TODO: docstring"""

    deployment_id: int
    file_ids: list[int]
    channel: int
    start_utc: datetime
    file_end_utc: datetime
    max_file_gap: float
    end_utc: datetime = None

    @property
    def has_ended(self):
        return self.end_utc is not None

    def file_gap(self, file_start_utc):
        if self.file_end_utc:
            return (file_start_utc - self.file_end_utc).total_seconds()
        else:
            return np.inf

    def end(self, end_utc):
        self.end_utc = end_utc

    def new_annotation(self, annot_start_utc, annot_end_utc):
        if annot_start_utc <= self.start_utc:
            self.start_utc = max(annot_end_utc, self.start_utc)

        else:
            self.end(annot_start_utc)

    def new_file(
        self,
        file_id: int,
        file_start_utc: datetime,
        file_end_utc: datetime,
    ):
        # if the period has ended, do nothing
        if self.has_ended:
            return

        # if the period starts *after* the new file, update its file attributes
        if self.start_utc >= file_start_utc:
            self.file_ids = [file_id]
            self.file_end_utc = file_end_utc

        # if the temporal gap to the previous file exceeds the maximum allowed gap, update the period's `end_utc` attribute
        elif self.file_gap(file_start_utc) > self.max_file_gap:
            self.end(self.file_end_utc)

        else:
            self.file_ids.append(file_id)
            self.file_end_utc = file_end_utc


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
        assert (
            channel in self.mono_periods
        ), "`new_file` must be called before `new_annotation`"

        # update the current mono time-period
        self.mono_periods[channel].new_annotation(annot_start_utc, annot_end_utc)

        # start a new mono time-period, if the current one has ended
        p = self.mono_periods[channel]
        if p.has_ended:
            self.mono_periods[channel] = MonoTimePeriod(
                deployment_id=self.deployment_id,
                max_file_gap=self.max_file_gap,
                file_ids=p.file_ids,
                channel=channel,
                file_end_utc=p.file_end_utc,
                start_utc=annot_end_utc,
            )

        # return the updated mono time-period (*not* the new one, if a new one was created)
        return deepcopy(p)

    def new_file(
        self,
        channel: int,
        file_id: int,
        file_start_utc: datetime,
        file_end_utc: datetime,
    ) -> MonoTimePeriod:
        # update the current mono time-period
        if channel in self.mono_periods:
            self.mono_periods[channel].new_file(file_id, file_start_utc, file_end_utc)

        # start a new mono time-period, if there is none or the current one has ended
        p = self.mono_periods.get(channel, None)
        if p is None or p.has_ended:
            self.mono_periods[channel] = MonoTimePeriod(
                deployment_id=self.deployment_id,
                max_file_gap=self.max_file_gap,
                file_ids=[file_id],
                channel=channel,
                file_end_utc=file_end_utc,
                start_utc=file_start_utc,
            )

            if p is None:
                p = self.mono_periods[channel]

        # return the updated mono time-period
        return deepcopy(p)


def _ensure_utc_start(files: pd.DataFrame, max_file_gap: float) -> pd.DataFrame:
    """Helper function to ensure all files have valid UTC start and end times"""
    is_na = files.start_utc.isna()

    # if all files have start and end times, return unchanged
    if is_na.sum() == 0:
        return files

    # pick the latest, valid end time
    end_utc = files.end_utc.max()
    if pd.isna(end_utc):
        end_utc = datetime(2000, 1, 1)

    # loop over files without start and end times, assigning them
    # arbitrary times with sufficient spacing to ensure they don't
    # get `chained` together
    gap = max(0.1, 2 * abs(max_file_gap))

    for idx, row in files.loc[is_na].iterrows():
        start_utc = end_utc + timedelta(seconds=gap)
        duration = row.num_samples / row.sample_rate
        end_utc = start_utc + timedelta(seconds=duration)
        files.loc[idx, "start_utc"] = start_utc
        files.loc[idx, "end_utc"] = end_utc

    return files


def find_unannotated_periods(
    files: pd.DataFrame, annots: pd.DataFrame = None, max_file_gap: float = 0.1
):
    """Find time periods without annotations.

    Notes:
     - Annotation UTC start times are derived by adding the within-file start time (`start`) to the file UTC start time

    Args:
        files: pandas.DataFrame
            Table of audio files. Must have columns `deployment_id`, `file_id`, `channel`, `start_utc`, `end_utc`.
        annots: pandas.DataFrame (optional)
            Table of annotations. Must have columns `deployment_id`, `file_id`, `channel`, `start`, `duration`.
        max_file_gap: float
            Maximum temporal gap between audiofiles in seconds.
            Inter-annotation gaps are allowed to span multiple audio files provided the temporal gap between the files is below this value.

    Returns:
        df: pd.DataFrame
            Time periods without annotations.
            TODO: list column names and dtypes
    """
    # make copies so we don't modify the input args
    files = files.copy()
    if annots is not None:
        annots = annots.copy() if len(annots) > 0 else None

    # ensure all files have UTC start times
    files = _ensure_utc_start(files, max_file_gap)

    # add start and end times to annotation table
    if annots is not None:
        files = files.reset_index().set_index("file_id")
        annots["start_utc"] = annots.apply(
            lambda r: files.loc[r.file_id].start_utc
            + timedelta(microseconds=r.start * 1e6),
            axis=1,
        )
        annots["end_utc"] = annots.apply(
            lambda r: r.start_utc + timedelta(microseconds=r.duration * 1e6), axis=1
        )

    # sort chronologically
    files.sort_values(by="start_utc", inplace=True)
    if annots is not None:
        annots.sort_values(by="start_utc", inplace=True)

    # reindex
    files = files.reset_index().set_index(["deployment_id", "start_utc", "end_utc"])
    if annots is not None:
        annots = annots.reset_index().set_index(
            ["deployment_id", "channel", "start_utc"]
        )

    # container for collecting inter-annotation time periods
    periods = []

    # loop over deployments
    for deployment_id, files_deploy in files.groupby(level=0):

        # start new stereo, inter-annotation time period
        stereo_period = StereoTimePeriod(deployment_id, max_file_gap)

        # loop over files
        for (_, file_start_utc, file_end_utc), file_row in files_deploy.iterrows():

            # loop over channels
            for channel in file_row.channel:

                # update the current stereo period with the new file
                p = stereo_period.new_file(
                    channel=channel,
                    file_id=file_row.file_id,
                    file_start_utc=file_start_utc,
                    file_end_utc=file_end_utc,
                )

                # if the period has ended, save it to the list
                if p.has_ended:
                    periods.append(p)

                if annots is None:
                    continue

                try:
                    # select annotations for current deployment and channel
                    annots_dc = annots.loc[deployment_id, channel]

                except KeyError:
                    # if there are none, proceed to the next channel
                    continue

                # select annotations that have start time within current file
                annots_file = annots_dc.loc[
                    (annots_dc.index >= file_start_utc)
                    & (annots_dc.index < file_end_utc)
                ]

                # loop over annotations
                for start_utc, row in annots_file.iterrows():
                    p = stereo_period.new_annotation(channel, start_utc, row.end_utc)

                    # if the period has ended, save it to the list
                    if p.has_ended:
                        periods.append(p)

        # end and save any periods that are still open
        for _, p in stereo_period.mono_periods.items():
            if not p.has_ended:
                p.end(file_end_utc)
                periods.append(p)

    # prep return table
    data = [
        (
            p.deployment_id,
            p.file_ids[0],
            p.file_ids,
            p.channel,
            p.start_utc,
            (p.end_utc - p.start_utc).total_seconds(),
        )
        for p in periods
    ]
    df = pd.DataFrame(
        data,
        columns=[
            "deployment_id",
            "file_id",
            "file_id_list",
            "channel",
            "start_utc",
            "duration",
        ],
    )

    # add `start` column
    files = files.reset_index().set_index("file_id")
    df["start"] = df.apply(
        lambda r: (r.start_utc - files.loc[r.file_id].start_utc).total_seconds(),
        axis=1,
    )

    # drop `start_utc` column
    df = df.drop(columns="start_utc")

    return df
