import pytest
import pandas as pd
from datetime import datetime, timezone


def test_generate_negatives(interfaces_with_taxonomy):
    """A fairly minimal test of the `generate_negatives` method"""
    file = interfaces_with_taxonomy["file"]
    job = interfaces_with_taxonomy["job"]
    annot = interfaces_with_taxonomy["annotation"]

    # add job with no target
    job.add({"taxonomy_id": 0})

    # add a couple of 10-s long audio files without timestamps
    frow = {
        "deployment_id": 0,
        "storage_id": 0,
        "sample_rate": 2000,
        "num_samples": 20000,
    }
    # add two files
    frow["filename"] = "file1.wav"
    file.add(frow)
    frow["filename"] = "file2.wav"
    file.add(frow)

    # add files to job
    job.add_file(0, 0, channel=0)
    job.add_file(0, 1, channel=0)

    # generate negatives
    annot.generate_negatives(0)

    # there should be two negatives, one for each file
    indices = annot.filter(negative=True).indices
    assert indices == [0, 1]

    # check the annotation data for these two negatives
    df = annot.get(
        indices,
        fields=["duration", "start", "excluded_label"],
        as_pandas=True,
        return_indices=True,
    )
    expected = """    duration  start excluded_label
id                                
0       10.0    0.0           None
1       10.0    0.0           None"""
    assert df.to_string() == expected

    # set the target for the job
    job.set(0, {"target": [("KW", "TC")]})

    # re-generate negatives
    annot.generate_negatives(0)

    # there should again be two negatives, AND excluded_label should now be same as job target
    indices = annot.reset_filter().filter(negative=True).indices
    assert indices == [0, 1]
    df = annot.get(
        indices,
        fields=["duration", "start", "excluded_label"],
        as_pandas=True,
        return_indices=True,
    )
    expected = """    duration  start excluded_label
id                                
0       10.0    0.0     [(KW, TC)]
1       10.0    0.0     [(KW, TC)]"""
    assert df.to_string() == expected

    # add an annotation, then re-generate negatives
    row = {
        "deployment_id": 0,
        "job_id": 0,
        "file_id": 0,
        "label": ("HW", "TC"),
        "duration": 2.0,
        "start": 4.0,
    }
    annot.add(row)
    annot.generate_negatives(0)

    # there should now be 3 negatives
    indices = annot.reset_filter().filter(negative=True).indices
    assert len(indices) == 3
    df = annot.get(
        indices,
        fields=["duration", "start", "label", "excluded_label"],
        as_pandas=True,
        return_indices=True,
    )
    expected = """    duration  start label excluded_label
id                                      
1        4.0    0.0  None     [(KW, TC)]
2        4.0    6.0  None     [(KW, TC)]
3       10.0    0.0  None     [(KW, TC)]"""
    assert df.to_string() == expected

    # filtering on exclude=(KW,*) returns the humpback call and the negatives
    indices = annot.reset_filter().filter(exclude=("KW", "*")).indices
    assert len(indices) == 4

    # negatives can be omitted by filtering on negative=False
    indices = annot.reset_filter().filter(exclude=("KW", "*"), negative=False).indices
    assert len(indices) == 1

    df = annot.get(
        indices=indices,
        fields=["duration", "start", "label", "excluded_label"],
        as_pandas=True,
        return_indices=True,
    )
    expected = """    duration  start     label excluded_label
id                                          
0        2.0    4.0  (HW, TC)           None"""
    assert df.to_string() == expected


def test_add_get_set_filter_data(interfaces_with_taxonomy):
    """Check that we can add data to, retrieve data from, and modify data in an AnnotationInterface instance"""

    job = interfaces_with_taxonomy["job"]
    tag = interfaces_with_taxonomy["tag"]
    annot = interfaces_with_taxonomy["annotation"]

    # add a job
    job.add({"taxonomy_id": 0})

    # attempt to add an annotation with invalid tags fails
    row = {
        "deployment_id": 0,
        "job_id": 0,
        "label": ("SRKW", "S01"),
        "start_utc": datetime(2022, 1, 1),
        "duration": 3.0,
        "start": 17.1,
        "tag": ["loud", "pretty"],
    }
    with pytest.raises(ValueError):
        annot.add(row)

    # add the tags
    tag.add({"name": "loud", "description": "a loud sound"})
    tag.add({"name": "pretty", "description": "a pretty sound"})
    tag.add({"name": "faint", "description": "a faint sound"})

    # try again
    annot.add(row)

    # add another annotation
    row = {
        "deployment_id": 0,
        "job_id": 0,
        "label": ("KW", "PC"),
        "start_utc": datetime(2023, 1, 1),
        "duration": 2.0,
        "start": 18.1,
        "tag": "faint",
        "granularity": "batch",
    }
    annot.add(row)

    # no condition
    idx = annot.reset_filter().filter().indices
    assert idx == [0, 1]

    # --- filter using condition ---
    idx = annot.reset_filter().filter({"label": ("SRKW", "*")}).indices
    assert idx == [0]

    idx = annot.reset_filter().filter({"granularity": "batch"}).indices
    assert idx == [1]

    idx = annot.reset_filter().filter({"tag": "pretty"}).indices
    assert idx == [0]

    # --- filter using keyword args ---
    idx = annot.reset_filter().filter(select=("SRKW", "*")).indices
    assert idx == [0]

    # note: finds descendant nodes too
    idx = annot.reset_filter().filter(select=("KW", "PC")).indices
    assert idx == [0, 1]

    idx = annot.reset_filter().filter(exclude=("SRKW", "*")).indices
    assert idx == []

    idx = annot.reset_filter().filter(exclude=("SRKW", "S01")).indices
    assert idx == []


def test_comprehensive_example(
    sqlite_database_with_taxonomy,
    one_deployment,
    one_storage_location,
    two_files,
):
    """A fairly comprehensive (and rather complex) test that we can add a broad range of
    annotations to the database and search for them using the filter method.
    """
    db = sqlite_database_with_taxonomy

    # add deployment
    db.deployment.add(one_deployment)

    # add storage location
    db.storage.add(one_storage_location)

    # add files
    for file in two_files:
        db.file.add(file)

    # add a job
    target = [("KW", "PC"), ("KW", "W")]
    job_data = {
        "taxonomy_id": 2,
        "annotator": "LL",
        "target": target,
        "is_exhaustive": True,
        "start_utc": datetime(2022, 10, 1),
        "end_utc": datetime(2023, 3, 1),
        "comments": "Vessel noise annotated opportunistically",
        "issues": [
            "start and end times may not always be accurate",
            "some KW sounds may have been incorrectly labelled as HW",
        ],
    }
    db.job.add(job_data)

    # link files
    db.job.add_file(0, 0)  # job:0 file:0
    db.job.add_file(0, 1)

    # add a tag
    db.tag.add({"name": "NEGATIVE", "description": "a negative sample"})

    # insert three annotations (id: 0, 1, 2)
    deployment_id = two_files[0]["deployment_id"]
    df = pd.DataFrame(
        {
            "deployment_id": [deployment_id, deployment_id, deployment_id],
            "job_id": [0, 0, 0],
            "file_id": [0, 0, 0],
            "channel": [0, 0, 0],
            "label": [("KW", "PC"), ("SRKW", "S01"), None],
            "tentative_label": [("SRKW", "S01"), None, None],
            "tag": [None, None, ["NEGATIVE"]],
            "duration_ms": [1300, 300000, 800],
            "start_ms": [30000, 21200, 1000],
            "freq_min_hz": [600, 700, 800],
            "freq_max_hz": [4400, 3300, 2200],
            "granularity": ["unit", "window", "window"],
            "comments": ["no additional observations", "", "this is a negative sample"],
        }
    )
    for _, row in df.iterrows():
        db.annotation.add(row)

    assert len(db.annotation) == 3

    # generate negatives
    db.annotation.generate_negatives(0)

    # check that annotation table now has 6 entries
    assert len(db.annotation) == 6

    # query the table and check that we get the correct data back
    fields = ["label", "start", "duration", "tag"]
    values_list = db.annotation.get(fields=fields)
    for i, values in enumerate(values_list):

        label, start, duration, tag = values
        end = start + duration

        if i == 0:
            assert label == ("KW", "PC")
            assert start == 30.0
            assert end == 31.3

        elif i == 1:
            assert label == ("SRKW", "S01")
            assert start == 21.2
            assert end == 321.2

        elif i == 2:
            assert label == None
            assert start == 1.0
            assert end == 1.8
            assert tag == ["NEGATIVE"]

        elif i > 2:
            assert label == None

            if i == 3:
                assert start == 0.0
                assert end == 1.0
            elif i == 4:
                assert start == 1.8
                assert end == 21.2
            elif i == 5:
                assert start == 21.177
                assert end == 300.0

    # test filtering
    indices = (
        db.annotation.reset_filter()
        .filter(select=("KW", "PC"), taxonomy_version=1)
        .indices
    )
    assert indices == [0, 1]

    indices = (
        db.annotation.reset_filter()
        .filter(select=("SRKW", "PC"), taxonomy_version=3)
        .indices
    )
    assert indices == [1]

    indices = (
        db.annotation.reset_filter()
        .filter(select=("SRKW", "S01"), taxonomy_version=2)
        .indices
    )
    assert indices == [1]

    indices = (
        db.annotation.reset_filter()
        .filter(select=("SRKW", "S01"), tentative=True, taxonomy_version=2)
        .indices
    )
    assert indices == [0, 1]

    # insert annotations with minimal required info (id: 6, 7, 8)
    df = pd.DataFrame(
        {
            "job_id": [0, 0, 0],
            "file_id": [1, 1, 1],
            "label": [("KW", "PC"), ("SRKW", "S01"), None],
        }
    )
    for _, row in df.iterrows():
        db.annotation.add(row)

    # define some more tags
    db.tag.add({"name": "noise", "description": "A sample with noise"})
    db.tag.add({"name": "Loud noise", "description": "A sample with loud noise"})

    # insert annotations with tags (id: 9,10)
    df = pd.DataFrame(
        {
            "job_id": [0, 0],
            "file_id": [1, 1],
            "label": [("SRKW", "PC"), ("SRKW", "PC")],
            "tag": [["noise"], ["Loud noise"]],
        }
    )
    for _, row in df.iterrows():
        db.annotation.add(row)

    # filter
    indices = (
        db.annotation.reset_filter()
        .filter(select=("SRKW", "PC"), taxonomy_version=3)
        .indices
    )
    assert indices == [1, 7, 9, 10]

    indices = (
        db.annotation.reset_filter()
        .filter(select=("SRKW", "PC"), taxonomy_version=3, tag="Loud noise")
        .indices
    )
    assert indices == [10]

    indices = (
        db.annotation.reset_filter()
        .filter(select=("SRKW", "PC"), taxonomy_version=3, tag="noise")
        .indices
    )
    assert indices == [9]

    indices = (
        db.annotation.reset_filter()
        .filter(select=("SRKW", "PC"), taxonomy_version=3, tag=["noise", "Loud noise"])
        .indices
    )
    assert indices == [9, 10]

    # retrieve some data and check that values are correct
    fields = [
        "job_id",
        "deployment_id",
        "file_id",
        "label",
        "tentative_label",
        "start_utc",
        "duration",
        "start",
        "freq_min_hz",
        "freq_max_hz",
        "channel",
        "granularity",
        "tag",
        "comments",
        "valid",
        "negative",
    ]
    df = db.annotation.get(fields=fields, indices=[0, 2, 4], as_pandas=True)

    row = df.iloc[0]
    assert row.job_id == 0
    assert row.deployment_id == 0
    assert row.file_id == 0
    assert row.label == ("KW", "PC")
    assert row.tentative_label == ("SRKW", "S01")
    assert row.start_utc == datetime(2022, 6, 24, 16, 40, 30, tzinfo=timezone.utc)
    assert row.duration == 1.3
    assert row.start == 30.0
    assert row.freq_min_hz == 600
    assert row.freq_max_hz == 4400
    assert row.channel == 0
    assert row.granularity == "unit"
    assert row.tag == None
    assert row.comments == "no additional observations"
    assert row.valid
    assert not row.negative

    row = df.iloc[1]
    assert row.job_id == 0
    assert row.deployment_id == 0
    assert row.file_id == 0
    assert row.label == None
    assert row.tentative_label == None
    assert row.start_utc == datetime(2022, 6, 24, 16, 40, 1, tzinfo=timezone.utc)
    assert row.duration == 0.8
    assert row.start == 1.0
    assert row.freq_min_hz == 800
    assert row.freq_max_hz == 2200
    assert row.channel == 0
    assert row.tag == ["NEGATIVE"]
    assert row.granularity == "window"
    assert row.comments == "this is a negative sample"
    assert row.valid
    assert not row.negative

    row = df.iloc[2]
    assert row.job_id == 0
    assert row.deployment_id == 0
    assert row.file_id == 0
    assert row.label == None
    assert row.tentative_label == None
    assert row.start_utc == datetime(
        2022, 6, 24, 16, 40, 1, 800000, tzinfo=timezone.utc
    )
    assert row.duration == 19.4
    assert row.start == 1.8
    assert row.freq_min_hz == 0
    assert row.freq_max_hz == 16000
    assert row.channel == 0
    assert row.tag == None
    assert row.granularity == "window"
    assert row.comments == None
    assert row.valid
    assert row.negative

    # perform another query and verify data
    indices = (
        db.annotation.reset_filter()
        .filter({"tag": "NEGATIVE"}, {"negative": True})
        .indices
    )
    df = db.annotation.get(
        indices,
        fields=[
            "file_id",
            "start",
            "duration",
            "freq_min_hz",
            "freq_max_hz",
            "comments",
        ],
        as_pandas=True,
    )

    file_ids = df.file_id.values.tolist()
    df["filename"] = db.file.get(file_ids, "filename", always_tuple=False)
    df["relative_path"] = db.file.get(file_ids, "relative_path", always_tuple=False)

    assert len(df) == 4

    filename1 = two_files[0]["filename"]
    filename2 = two_files[1]["filename"]
    relative_path = two_files[0]["relative_path"]

    for idx, row in df.iterrows():
        if idx == 0:
            assert row.filename == filename1
            assert row.relative_path == relative_path
            assert row.start == 1.0
            assert row.duration == 0.8
            assert row.freq_min_hz == 800
            assert row.freq_max_hz == 2200

        elif idx == 1:
            assert row.filename == filename1
            assert row.relative_path == relative_path
            assert row.start == 0.0
            assert row.duration == 1.0
            assert row.freq_min_hz == 0
            assert row.freq_max_hz == 16000

        elif idx == 2:
            assert row.filename == filename1
            assert row.relative_path == relative_path
            assert row.start == 1.8
            assert row.duration == 19.4
            assert row.freq_min_hz == 0
            assert row.freq_max_hz == 16000

        elif idx == 3:
            assert row.filename == filename2
            assert row.relative_path == relative_path
            assert row.start == 21.177
            assert row.duration == 278.823
            assert row.freq_min_hz == 0
            assert row.freq_max_hz == 16000


"""
    # insert annotations with ambiguous labels
    annot_tbl = pd.DataFrame(
        {
            "file_id": [2, 2, 2, 2, 2],  # id: 12,13,14,15,16
            "sound_source": ["SRKW", "SRKW", "NRKW", "KW", "KW"],
            "sound_type": ["S01", "S02", "N01", "PC", "PC"],
            "ambiguous_sound_source": [None, None, None, "SRKW,NRKW", None],
            "ambiguous_sound_type": [None, None, None, "S01,S02,S16,N01,N22", "PC,W"],
        }
    )
    annot_ids = kdb.add_annotations(conn, annot_tbl=annot_tbl, job_id=1, error="ignore")
    rows = c.execute(
        f"SELECT label_id, ambiguous_label_id FROM annotation WHERE id IN {list_to_str(annot_ids)}"
    ).fetchall()
    assert rows[0] == (36, "[null]")
    assert rows[1] == (37, "[null]")
    assert rows[2] == (43, "[null]")
    assert rows[3] == (24, "[36, 37, 43]")
    assert rows[4] == (24, "[24, 25]")

    # filter on label_id and tentative_label_id only
    idx = kdb.filter_annotation(
        conn, source_type=("SRKW", "S02"), tentative=True, taxonomy_id=2
    )
    assert len(idx) == 1

    # filter again, now also including ambiguous label assignments
    idx = kdb.filter_annotation(
        conn, source_type=("SRKW", "S02"), tentative=True, ambiguous=True, taxonomy_id=2
    )
    assert len(idx) == 2

    # invert the filter
    idx = kdb.filter_annotation(
        conn, source_type=("SRKW", "PC"), invert=True, tentative=True, taxonomy_id=2
    )
    assert idx[-1] == annot_ids[2]
    with pytest.raises(NotImplementedError):
        idx = kdb.filter_annotation(
            conn,
            source_type=("SRKW", "PC"),
            invert=True,
            tentative=True,
            ambiguous=True,
            taxonomy_id=2,
        )
        # assert idx[-1] == annot_ids[2]

    # insert a humpback annotation
    annot_tbl = pd.DataFrame(
        {
            "file_id": [2],  # id:17
            "sound_source": ["HW"],
            "sound_type": ["TC"],
        }
    )
    annot_ids = kdb.add_annotations(conn, annot_tbl=annot_tbl, job_id=1)

    # filter using @invert=True
    idx = kdb.filter_annotation(
        conn, source_type=("KW", "%"), invert=True, taxonomy_id=3
    )

    assert idx == [3, 9, 17]

    # filter on negatives
    idx = kdb.filter_negative(conn, source_type=("KW", "%"), taxonomy_id=3)
    assert len(idx) == 0
    # since only KW,PC and KW,W where subject to systematic annotation (while KW,EC wasn't), the auto-generated
    # negatives are excluded from this search

    idx = kdb.filter_negative(conn, source_type=("KW", "PC"), taxonomy_id=3)
    assert idx == [4, 5, 6]

    idx = kdb.filter_negative(conn, source_type=("HW", "%"), taxonomy_id=3)
    assert len(idx) == 0

    # insert annotations with excluded labels
    annot_tbl = pd.DataFrame(
        {
            "file_id": [2, 2, 2],  # id:18,19,20
            "sound_source": ["Unknown", "Unknown", "KW"],
            "excluded_sound_source": ["KW", ["KW", "HW"], "SRKW"],
            "excluded_sound_type": ["PC", "Unknown", "S01"],
        }
    )
    annot_ids = kdb.add_annotations(conn, annot_tbl=annot_tbl, job_id=1, error="ignore")

    # check that the first two annotations (18,19) are returned when searching for non-KW annotations
    idx = kdb.filter_annotation(
        conn, source_type=("KW", "%"), invert=True, taxonomy_id=2
    )
    assert idx == [3, 9, 17, 18, 19]

    # check that all three annotations (18,19,20) are returned when searching for non-(SRKW,S01) annotations
    idx = kdb.filter_annotation(
        conn, source_type=("SRKW", "S01"), invert=True, taxonomy_id=2
    )
    assert idx == [3, 9, 13, 14, 17, 18, 19, 20]

    # check that the first two annotations (18,19) are returned when searching for non-(SRKW,S02) annotations
    idx = kdb.filter_annotation(
        conn, source_type=("SRKW", "S02"), invert=True, taxonomy_id=2
    )
    assert idx == [2, 3, 8, 9, 12, 14, 17, 18, 19]

    # check that second and third annotations (19,20) is returned when searching for non-HW annotations
    idx = kdb.filter_annotation(
        conn, source_type=("HW", "%"), invert=True, taxonomy_id=2
    )
    assert idx == [1, 2, 3, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 19, 20]

    # check that `exclude` arg works as intended when filtering
    # expecting query to return annotations 13, 14, and 20
    idx = kdb.filter_annotation(
        conn, source_type=("KW", "%"), exclude=("SRKW", "S01"), taxonomy_id=2
    )
    assert idx == [13, 14, 20]
"""
