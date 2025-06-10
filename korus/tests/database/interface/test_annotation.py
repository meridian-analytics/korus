import pytest
import pandas as pd
from datetime import datetime


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
    fields = ["label","start","duration","tag"]
    values_list = db.annotation.get(fields=fields)
    for i,values in enumerate(values_list):

        label,start,duration,tag = values
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


"""
    # insert a few annotations
    annot_tbl = pd.DataFrame(
        {
            "file_id": [1, 1, 1],  # id:1,2,3
            "channel": [0, 0, 0],
            "sound_source": ["KW", "SRKW", None],
            "sound_type": ["PC", "S01", None],
            "tentative_sound_source": ["SRKW", None, None],
            "tentative_sound_type": ["S01", None, None],
            "tag": [None, None, ["NEGATIVE"]],
            "duration_ms": [1300, 300000, 800],
            "start_ms": [30000, 21200, 1000],
            "freq_min_hz": [600, 700, None],
            "freq_max_hz": [4400, 3300, None],
            "granularity": ["unit", "window", "window"],
            "comments": ["no additional observations", "", "this is a negative sample"],
        }
    )
    annot_ids = kdb.add_annotations(conn, annot_tbl=annot_tbl, job_id=1)
    neg_ids = kdb.add_negatives(conn, job_id=1)  # this adds 3 negatives

    query = "SELECT label_id,start_ms,duration_ms,file_id,tag_id FROM annotation"
    rows = c.execute(query).fetchall()
    for i, row in enumerate(rows):
        l = row[0]
        f = row[3]
        ss, st = c.execute(
            f"SELECT sound_source_tag,sound_type_tag FROM label WHERE id = '{l}'"
        ).fetchall()[0]
        start = row[1] / 1000.0
        end = start + row[2] / 1000.0
        tag_id = json.loads(row[4])

        if i == 0:
            assert ss == "KW"
            assert st == "PC"
            assert start == 30.0
            assert end == 31.3

        elif i == 1:
            assert ss == "SRKW"
            assert st == "S01"
            assert start == 21.2
            assert end == 321.2

        elif i == 2:
            assert ss == None
            assert st == None
            assert tag_id == [2]
            assert start == 1.0
            assert end == 1.8

        elif i > 2:
            assert ss == None
            assert st == None
            assert tag_id == [1]

            if i == 3:
                assert start == 0.0
                assert end == 1.0
            elif i == 4:
                assert start == 1.8
                assert end == 21.2
            elif i == 5:
                assert start == 21.177
                assert end == 300.0

    # test filter_annotation function
    rows = kdb.filter_annotation(conn, source_type=("KW", "PC"), taxonomy_id=1)
    assert rows == [1, 2]

    rows = kdb.filter_annotation(conn, source_type=("SRKW", "PC"), taxonomy_id=3)
    assert rows == [2]

    rows = kdb.filter_annotation(
        conn, source_type=("SRKW", "S01"), taxonomy_id=2, tentative=False
    )
    assert rows == [2]

    rows = kdb.filter_annotation(
        conn, source_type=("SRKW", "S01"), taxonomy_id=2, tentative=True
    )
    assert rows == [1, 2]

    # see all attached databases
    ##rows = conn.execute("SELECT * FROM pragma_database_list").fetchall()
    ##print(rows)

    # insert annotations with minimal required info
    annot_tbl = pd.DataFrame(
        {
            "file_id": [2, 2, 2],  # id:7,8,9
            "sound_source": ["KW", "SRKW", None],
            "sound_type": ["PC", "S01", None],
        }
    )
    annot_ids = kdb.add_annotations(conn, annot_tbl=annot_tbl, job_id=1)

    # define some tags
    v = {"name": "noise", "description": "A sample with noise"}
    c = kdb.insert_row(conn, table_name="tag", values=v)
    v = {"name": "Loud noise", "description": "A sample with loud noise"}
    c = kdb.insert_row(conn, table_name="tag", values=v)

    # insert annotations with tags
    annot_tbl = pd.DataFrame(
        {
            "file_id": [2, 2],  # id: 10,11
            "sound_source": ["SRKW", "SRKW"],
            "sound_type": ["PC", "PC"],
            "tag": ["noise", "Loud noise"],
        }
    )
    annot_ids = kdb.add_annotations(conn, annot_tbl=annot_tbl, job_id=1)

    rows = kdb.filter_annotation(conn, source_type=("SRKW", "PC"), taxonomy_id=3)
    assert rows == [2, 8, 10, 11]

    rows = kdb.filter_annotation(
        conn, source_type=("SRKW", "PC"), taxonomy_id=3, tag="Loud noise"
    )
    assert rows == [11]

    rows = kdb.filter_annotation(
        conn, source_type=("SRKW", "PC"), taxonomy_id=3, tag="noise"
    )
    assert rows == [10]

    rows = kdb.filter_annotation(
        conn, source_type=("SRKW", "PC"), taxonomy_id=2, tag=["noise", "Loud noise"]
    )
    assert rows == [10, 11]

    annot_tbl = kdb.get_annotations(conn, indices=[1, 3, 5])
    path = os.path.join(path_to_assets, "compr-example-test-annot1.csv")
    expected = pd.read_csv(path)
    expected = expected.astype(
        {
            "start_utc": "datetime64[ns]",
            "tentative_sound_source": "object",
            "tentative_sound_type": "object",
            "machine_prediction": "object",
            "ambiguous_label": "object",
        }
    )

    # expected.ambiguous_label = expected.ambiguous_label.fillna("")
    def _decode_tag(x):
        if isinstance(x, float) and np.isnan(x):
            return None
        else:
            return json.loads(x)

    expected.tag = expected.tag.apply(lambda x: _decode_tag(x))
    pd.testing.assert_frame_equal(
        annot_tbl[expected.columns], expected[expected.columns]
    )

    indices_0 = kdb.filter_annotation(conn, tag="NEGATIVE")
    indices_1 = kdb.filter_annotation(conn, tag=ktb.AUTO_NEG)
    df_0 = kdb.get_annotations(conn, indices_0, format="ketos", label=0)
    df_1 = kdb.get_annotations(conn, indices_1, format="ketos", label=1)
    df_kt = pd.concat([df_0, df_1])

    # temporary fix: reformat to match expectatin
    df_kt.reset_index(inplace=True)
    df_kt.drop(columns=["annot_id", "top_path"], inplace=True)

    path = os.path.join(path_to_assets, "compr-example-test-annot2.csv")
    expected = pd.read_csv(path)
    pd.testing.assert_frame_equal(df_kt, expected)

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
