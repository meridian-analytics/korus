from datetime import datetime, timedelta
from ..file import FileInterface


def _get_file_ids(
    deployment_id: int, start_utc: datetime, end_utc: datetime, file: FileInterface
) -> list[int]:
    """Helper function for finding the audiofiles that overlap with a specified time range"""
    # search for files that overlap with the specified time range
    condition = {
        "start_utc": (None, end_utc),
        "end_utc": (start_utc, None),
        "deployment_id": deployment_id,
    }
    indices = file.reset_filter().filter(condition).indices

    # get the file UTC start times
    file_start_times = file.get(indices=indices, fields="start_utc", always_tuple=False)

    # sort chronologically
    sorted_indices = [
        i for i, _ in sorted(zip(indices, file_start_times), key=lambda pair: pair[1])
    ]

    return sorted_indices


def validate_annotation(row: dict, file: FileInterface) -> dict:
    row = validate_deployment_id(row, file)
    row = validate_timestamps(row, file)
    row = validate_file_id(row, file)
    row = validate_duration(row, file)
    row = validate_frequency(row, file)
    return row


def validate_file_id(row: dict, file: FileInterface) -> dict:
    """Helper function for validating file IDs.
    If annotation spans multiple files, attempt to determine the IDs of the secondary files, if not provided.
    """
    file_id = row.get("file_id", None)
    file_id_list = row.get("file_id_list", None)
    num_files = row.get("num_files", None)

    if file_id_list == []:
        file_id_list = None

    if file_id is None and file_id_list is None:
        return row

    if file_id is None:
        file_id = file_id_list[0]

    if file_id_list is None:
        if "start_utc" not in row or "duration_ms" not in row:
            file_id_list = [file_id]

        else:
            deployment_id = file.get(
                row["file_id"], fields="deployment_id", always_tuple=False
            )[0]
            start_utc = row["start_utc"]
            end_utc = start_utc + timedelta(microseconds=row["duration_ms"] * 1e3)
            file_id_list = _get_file_ids(deployment_id, start_utc, end_utc, file)

    if num_files is None:
        num_files = len(file_id_list)

    assert num_files == len(file_id_list), ""
    assert file_id in file_id_list, ""

    row["file_id"] = file_id
    row["file_id_list"] = file_id_list
    row["num_files"] = num_files

    return row


def validate_deployment_id(row: dict, file: FileInterface) -> dict:
    """Helper function for validating deployment ID.
    Raises AssertionError, if inconsistent deployment IDs are encountered.
    """
    if row.get("file_id", -1) == -1:
        return row

    # look up file metadata
    deployment_id = file.get(
        row["file_id"], fields="deployment_id", always_tuple=False
    )[0]

    if "deployment_id" not in row:
        row["deployment_id"] = deployment_id

    x = row["deployment_id"]
    assert_msg = f"The specified deployment ID ({x}) does not match the recorded deployment ID of the audiofile ({deployment_id})"
    assert x == deployment_id, assert_msg

    return row


def validate_timestamps(row: dict, file: FileInterface) -> dict:
    """Helper function for validating and completing annotations timestamps.
    Raises AssertionError, if the within-file offset is not specified or cannot be deduced.
    Raises ValueError, if inconsistent timestamps are encountered.
    """
    if "file_id" in row:
        # look up file metadata
        file_start_utc, sample_rate, num_samples = file.get(
            indices=row["file_id"], fields=["start_utc", "sample_rate", "num_samples"]
        )[0]

        file_duration = float(num_samples / sample_rate)

    else:
        file_start_utc = None
        file_duration = None

    # default to zero within-file offset, if neither timestamp has been specified
    if "start_utc" not in row and "start_ms" not in row:
        row["start_ms"] = 0

    # if within-file offset is unknown, derive it from the UTC start time
    if "start_ms" not in row and file_start_utc is not None:
        row["start_ms"] = int((row["start_utc"] - file_start_utc).total_seconds() * 1e3)

    # if UTC start time is unknown, derive it from the within-file offset
    elif "start_utc" not in row and file_start_utc is not None:
        row["start_utc"] = file_start_utc + timedelta(
            microseconds=row["start_ms"] * 1e3
        )

    # for audiofiles lacking a timestamp, the within-file offset must have a non-null value
    if file_start_utc is None:
        assert_msg = "The within-file annotation start time must be specified for audiofiles with unknown UTC start times"
        assert "start_ms" in row, assert_msg

    else:
        # check that timestamps are internally consistent
        delta_ms = row["start_ms"] - int(
            (row["start_utc"] - file_start_utc).total_seconds() * 1e3
        )
        if delta_ms != 0:
            err_msg = f"Data have inconsistent timestamps. Audiofile UTC start time: {file_start_utc} | Annotation UTC start time: {row['start_utc']} | Annotation within-file start time: {row['start_ms']:.0} ms"
            raise ValueError(err_msg)

    # check that annotation starts within file
    if file_duration is not None:
        assert row["start_ms"] >= 0, "Within-file offset cannot be a negative"
        assert (
            row["start_ms"] <= file_duration * 1e3
        ), "Within-file offset cannot exceed file duration"

    return row


def validate_duration(row: dict, file: FileInterface) -> dict:
    """Helper function for validating or inferring the annotation duration"""
    if "duration_ms" not in row and "file_id_list" in row:

        file_ids = row["file_id_list"]

        if len(file_ids) == 1:
            row["duration_ms"] = (
                int(file.get_duration(file_ids)[0] * 1e3) - row["start_ms"]
            )

        else:
            end_times = file.get(indices=file_ids, fields="end_utc", always_tuple=False)

            assert_msg = "Unable to infer duration of annotation that spans multiple audiofiles, some of which do not have timestamps"
            assert None not in end_times, assert_msg

            end_utc = max(end_times)
            row["duration_ms"] = int((end_utc - row["start_utc"]).total_seconds() * 1e3)

    return row


def validate_frequency(row: dict, file: FileInterface) -> dict:
    """Helper function for validating or inferring frequency limits"""
    nyquist_freq = None

    if "freq_min_hz" not in row:
        row["freq_min_hz"] = 0

    if "file_id" in row:
        sr = file.get(indices=row["file_id"], fields="sample_rate", always_tuple=False)[
            0
        ]
        nyquist_freq = sr // 2

        if "freq_max_hz" not in row:
            row["freq_max_hz"] = nyquist_freq

    assert_msg = f"Lower frequency limit exceeds Nyquist frequency"
    assert nyquist_freq is None or row["freq_min_hz"] <= nyquist_freq, assert_msg

    if row.get("freq_max_hz", None) is not None:
        freq_min = row["freq_min_hz"]
        freq_max = row["freq_max_hz"]

        assert_msg = f"Upper frequency limit ({freq_max:.0f} Hz) exceeds Nyquist frequency ({nyquist_freq:.0f} Hz)"
        assert nyquist_freq is None or freq_max <= nyquist_freq, assert_msg

        assert_msg = f"Upper frequency limit ({freq_max:.0f} Hz) is less than the lower frequency limit ({freq_min:.0f} Hz)"
        assert freq_max >= freq_min, assert_msg

    return row
