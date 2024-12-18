import os
import pytest
import json
import pandas as pd
import numpy as np
import korus.db as kdb
import korus.db_util.table as ktb
from korus.util import list_to_str


current_dir = os.path.dirname(os.path.realpath(__file__))
path_to_assets = os.path.join(current_dir, "assets")
path_to_tmp = os.path.join(path_to_assets, "tmp")


if not os.path.exists(path_to_tmp):
    os.makedirs(path_to_tmp)


def test_create_db():
    """Test that we can create an empty database"""
    path = os.path.join(path_to_tmp, "empty.sqlite")
    conn = kdb.create_db(path)
    conn.close()
    assert os.path.exists(path)
    os.remove(path)

def test_get_taxonomy(basic_db):
    """Test that we can use the get_taxonomy function to retrieve taxonomies
    OBS: This test does not make any checks on the contents of the taxonomies 
    """
    (conn, path) = basic_db
    tax_latest = kdb.get_taxonomy(conn)
    assert tax_latest.version == 4
    tax_v2 = kdb.get_taxonomy(conn, taxonomy_id=2)
    assert tax_v2.version == 2

def test_get_label_id(basic_db):
    """Test that we can use get_label_id function to retrieve correct label indices"""
    (conn, path) = basic_db

    tax_id = 2

    c = conn.cursor()
    query = f"SELECT id,sound_source_tag,sound_type_tag FROM label WHERE taxonomy_id = {tax_id}"
    rows = c.execute(query).fetchall()
    df = pd.DataFrame({
        "id": [r[0] for r in rows],
        "ss": [r[1] for r in rows],
        "st": [r[2] for r in rows],
    })
    df.set_index("id", inplace=True)

    l = kdb.get_label_id(conn, ("SRKW","S01"), taxonomy_id=tax_id, ascend=False, descend=False)
    assert df.loc[l].ss == "SRKW"
    assert df.loc[l].st == "S01"

    l_list = kdb.get_label_id(conn, ("SRKW","%"), taxonomy_id=tax_id, ascend=False, descend=False)
    for l in l_list:
        assert df.loc[l].ss == "SRKW"
        assert df.loc[l].st in ["S01", "S02", "PC", "CK", "W", "TC", "Unknown"]

    l_list = kdb.get_label_id(conn, ("SRKW","S01"), taxonomy_id=tax_id, ascend=True, descend=False)
    for l in l_list:
        assert df.loc[l].ss in ["SRKW", "KW", "Mammal", "Bio", "Unknown"]
        assert df.loc[l].st in ["S01", "PC", "TC", "Unknown"]

    l_list = kdb.get_label_id(conn, ("KW","PC"), taxonomy_id=tax_id, ascend=False, descend=True)
    for l in l_list:
        assert df.loc[l].ss in ["KW", "SRKW", "NRKW"]
        assert df.loc[l].st in ["PC", "S01", "S02", "N01"]

    with pytest.raises(ValueError):
        kdb.get_label_id(conn, ("Fish","PC"), taxonomy_id=tax_id, ascend=False, descend=True)

    with pytest.raises(ValueError):
        kdb.get_label_id(conn, ("Fish","%"), taxonomy_id=tax_id, ascend=False, descend=True)

    with pytest.raises(ValueError):
        kdb.get_label_id(conn, ("%","LoudSound"), taxonomy_id=tax_id, ascend=False, descend=True)


def test_insert_deployment(basic_db, deploy_data):
    """ Test that we can add a deployment to the database"""
    (conn, path) = basic_db
    c = conn.cursor()
    c = kdb.insert_row(conn, table_name="deployment", values=deploy_data)


def test_insert_files(basic_db, deploy_data, file_data):
    """ Test that we can add audio files to the database"""
    (conn, path) = basic_db
    c = conn.cursor()
    c = kdb.insert_row(conn, table_name="deployment", values=deploy_data)

    v = {
        "name": "laptop",
        "path": "/",
        "description": "data from my latest deployments",
    }
    c = kdb.insert_row(conn, table_name="storage", values=v)

    for v in file_data:
        kdb.insert_row(conn, table_name="file", values=v)


def test_comprehensive_example(basic_db, deploy_data, file_data):
    """ Testing a fairly comprehensive example of adding annotations to the database
    TODO: refactor this test into multiple smaller tests.
    """
    (conn, path) = basic_db
    c = conn.cursor()
    c = kdb.insert_row(conn, table_name="deployment", values=deploy_data)

    v = {
        "name": "laptop",
        "path": "/",
        "description": "data from my latest deployments",
    }
    c = kdb.insert_row(conn, table_name="storage", values=v)

    for v in file_data:
        kdb.insert_row(conn, table_name="file", values=v)

    # insert job
    primary_sound = kdb.get_label_id(
        conn, 
        source_type=[("KW","PC"), ("KW","W")],
        taxonomy_id=2,
        always_list=True
    )
    background_sound = kdb.get_label_id(
        conn, 
        source_type=("%","CK"),
        taxonomy_id=2,
        always_list=True
    )
    v = {
        "taxonomy_id": 2,
        "model_id": None,
        "annotator": "LL",
        "primary_sound": json.dumps(primary_sound),
        "background_sound": json.dumps(background_sound),
        "is_exhaustive": 1,
        "configuration": None,
        "start_utc": "2022-10",
        "end_utc": "2023-03",
        "by_human": 1,
        "by_machine": 0,
        "comments": "Vessel noise annotated opportunistically",
        "issues": json.dumps(["start and end times may not always be accurate", "some KW sounds may have been incorrectly labelled as HW"]),
    }
    c = kdb.insert_row(conn, table_name="job", values=v)

    # link files
    # get deployment id
    query = """
        SELECT 
            id
        FROM 
            deployment 
        WHERE 
            owner LIKE 'OceanResearch' 
            AND name LIKE 'WestPoint'
            AND start_utc >= '2022-01-01'
            AND end_utc <= '2022-12-31'
    """
    rows = c.execute(query).fetchall()
    deploy_id = rows[0][0]

    # get file_id
    for v in file_data:
        fname = v["filename"]

        query = f"""
            SELECT 
                id
            FROM 
                file 
            WHERE 
                deployment_id = '{deploy_id}' 
                AND filename LIKE '{fname}'
        """
        rows = c.execute(query).fetchall()
        file_id = rows[0][0]

        # link file to job
        v = {
            "job_id": 1,
            "file_id": file_id,
            "channel": 0
        }
        c = kdb.insert_row(conn, table_name="file_job_relation", values=v)

    # define some tags
    v = {
        "name": "NEGATIVE",
        "description": "A negative sample"
    }
    c = kdb.insert_row(conn, table_name="tag", values=v)

    #insert annotation using add_annotation function
    annot_tbl = pd.DataFrame({
        "file_id": [1,1,1],  #id:1,2,3
        "channel": [0,0,0],
        "sound_source": ["KW","SRKW",None],
        "sound_type": ["PC","S01",None],
        "tentative_sound_source": ["SRKW",None,None],
        "tentative_sound_type": ["S01",None,None],
        "tag": [None,None,["NEGATIVE"]],
        "duration_ms": [1300,300000,800],
        "start_ms": [30000,21200,1000],
        "freq_min_hz": [600,700,None],
        "freq_max_hz": [4400,3300,None],
        "granularity": ["unit","window","window"],
        "comments": ["no additional observations","","this is a negative sample"]
    })
    annot_ids = kdb.add_annotations(conn, annot_tbl=annot_tbl, job_id=1)
    neg_ids = kdb.add_negatives(conn, job_id=1) #this adds 3 negatives

    query = "SELECT label_id,start_ms,duration_ms,file_id,tag_id FROM annotation"
    rows = c.execute(query).fetchall()
    for i,row in enumerate(rows):
        l = row[0]
        f = row[3]
        ss,st = c.execute(f"SELECT sound_source_tag,sound_type_tag FROM label WHERE id = '{l}'").fetchall()[0]
        start = row[1]/1000.
        end = start + row[2]/1000.
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

    #test filter_annotation function
    rows = kdb.filter_annotation(conn, source_type=("KW","PC"), taxonomy_id=1)
    assert rows == [1, 2]

    rows = kdb.filter_annotation(conn, source_type=("SRKW","PC"), taxonomy_id=3)
    assert rows == [2]

    rows = kdb.filter_annotation(conn, source_type=("SRKW","S01"), taxonomy_id=2, tentative=False)
    assert rows == [2]

    rows = kdb.filter_annotation(conn, source_type=("SRKW","S01"), taxonomy_id=2, tentative=True)
    assert rows == [1, 2]

    # see all attached databases
    ##rows = conn.execute("SELECT * FROM pragma_database_list").fetchall()
    ##print(rows)

    #insert annotations with minimal required info
    annot_tbl = pd.DataFrame({
        "file_id": [2,2,2], #id:7,8,9
        "sound_source": ["KW","SRKW",None],
        "sound_type": ["PC","S01",None],
    })
    annot_ids = kdb.add_annotations(conn, annot_tbl=annot_tbl, job_id=1)

    # define some tags
    v = {
        "name": "noise",
        "description": "A sample with noise"
    }
    c = kdb.insert_row(conn, table_name="tag", values=v)
    v = {
        "name": "Loud noise",
        "description": "A sample with loud noise"
    }
    c = kdb.insert_row(conn, table_name="tag", values=v)

    #insert annotations with tags
    annot_tbl = pd.DataFrame({
        "file_id": [2,2], #id: 10,11
        "sound_source": ["SRKW","SRKW"],
        "sound_type": ["PC","PC"],
        "tag": ["noise", "Loud noise"]
    })
    annot_ids = kdb.add_annotations(conn, annot_tbl=annot_tbl, job_id=1)

    rows = kdb.filter_annotation(conn, source_type=("SRKW","PC"), taxonomy_id=3)
    assert rows == [2, 8, 10, 11]

    rows = kdb.filter_annotation(conn, source_type=("SRKW","PC"), taxonomy_id=3, tag="Loud noise")
    assert rows == [11]

    rows = kdb.filter_annotation(conn, source_type=("SRKW","PC"), taxonomy_id=3, tag="noise")
    assert rows == [10]

    rows = kdb.filter_annotation(conn, source_type=("SRKW","PC"), taxonomy_id=2, tag=["noise", "Loud noise"])
    assert rows == [10, 11]

    annot_tbl = kdb.get_annotations(conn, indices=[1,3,5])
    path = os.path.join(path_to_assets, "compr-example-test-annot1.csv")
    expected = pd.read_csv(path)
    expected = expected.astype({
        "start_utc": "datetime64[ns]", 
        "tentative_sound_source": "object",
        "tentative_sound_type": "object",
        "machine_prediction": "object",
        "ambiguous_label": "object",
    })
    #expected.ambiguous_label = expected.ambiguous_label.fillna("")
    def _decode_tag(x):
        if isinstance(x, float) and np.isnan(x):
            return None
        else:
            return json.loads(x)
    expected.tag = expected.tag.apply(lambda x: _decode_tag(x))
    pd.testing.assert_frame_equal(annot_tbl[expected.columns], expected[expected.columns])


    indices_0 = kdb.filter_annotation(conn, tag="NEGATIVE")
    indices_1 = kdb.filter_annotation(conn, tag=ktb.AUTO_NEG)
    df_0 = kdb.get_annotations(conn, indices_0, format="ketos", label=0)
    df_1 = kdb.get_annotations(conn, indices_1, format="ketos", label=1)
    df_kt = pd.concat([df_0, df_1])

    # temporary fix: reformat to match expectatin
    df_kt.reset_index(inplace=True)
    df_kt.drop(columns=["annot_id","korus_id","top_path"], inplace=True)

    path = os.path.join(path_to_assets, "compr-example-test-annot2.csv")
    expected = pd.read_csv(path)
    pd.testing.assert_frame_equal(df_kt, expected)


    #insert annotations with ambiguous labels
    annot_tbl = pd.DataFrame({
        "file_id": [2, 2, 2, 2, 2],#id: 12,13,14,15,16
        "sound_source": ["SRKW", "SRKW", "NRKW", "KW", "KW"],
        "sound_type": ["S01", "S02", "N01", "PC", "PC"],
        "ambiguous_sound_source": [None, None, None, "SRKW,NRKW", None],
        "ambiguous_sound_type": [None, None, None, "S01,S02,S16,N01,N22", "PC,W"],
    })
    annot_ids = kdb.add_annotations(conn, annot_tbl=annot_tbl, job_id=1, error="ignore")
    rows = c.execute(f"SELECT label_id, ambiguous_label_id FROM annotation WHERE id IN {list_to_str(annot_ids)}").fetchall()
    assert rows[0] == (36, '[null]')
    assert rows[1] == (37, '[null]')
    assert rows[2] == (43, '[null]')
    assert rows[3] == (24, '[36, 37, 43]')
    assert rows[4] == (24, '[24, 25]')

    #filter on label_id and tentative_label_id only
    idx = kdb.filter_annotation(conn, source_type=("SRKW","S02"), tentative=True, taxonomy_id=2)
    assert len(idx) == 1

    #filter again, now also including ambiguous label assignments
    idx = kdb.filter_annotation(conn, source_type=("SRKW","S02"), tentative=True, ambiguous=True, taxonomy_id=2)
    assert len(idx) == 2

    #invert the filter
    idx = kdb.filter_annotation(conn, source_type=("SRKW","PC"), invert=True, tentative=True, taxonomy_id=2)
    assert idx[-1] == annot_ids[2]
    with pytest.raises(NotImplementedError):
        idx = kdb.filter_annotation(conn, source_type=("SRKW","PC"), invert=True, tentative=True, ambiguous=True, taxonomy_id=2)
        #assert idx[-1] == annot_ids[2]
 
    #insert a humpback annotation
    annot_tbl = pd.DataFrame({
        "file_id": [2], #id:17
        "sound_source": ["HW"],
        "sound_type": ["TC"],
    })
    annot_ids = kdb.add_annotations(conn, annot_tbl=annot_tbl, job_id=1)

    #filter using @invert=True 
    idx = kdb.filter_annotation(conn, source_type=("KW","%"), invert=True, taxonomy_id=3)

    assert idx == [3, 9, 17] 

    #filter on negatives
    idx = kdb.filter_negative(conn, source_type=("KW","%"), taxonomy_id=3)
    assert len(idx) == 0
    # since only KW,PC and KW,W where subject to systematic annotation (while KW,EC wasn't), the auto-generated 
    # negatives are excluded from this search

    idx = kdb.filter_negative(conn, source_type=("KW","PC"), taxonomy_id=3)
    assert idx == [4, 5, 6]

    idx = kdb.filter_negative(conn, source_type=("HW","%"), taxonomy_id=3)
    assert len(idx) == 0


    #insert annotations with excluded labels
    annot_tbl = pd.DataFrame({
        "file_id": [2, 2, 2], #id:18,19,20
        "sound_source": ["Unknown", "Unknown", "KW"],
        "excluded_sound_source": ["KW", ["KW","HW"], "SRKW"],
        "excluded_sound_type": ["PC", "Unknown", "S01"],
    })
    annot_ids = kdb.add_annotations(conn, annot_tbl=annot_tbl, job_id=1, error="ignore")

    # check that the first two annotations (18,19) are returned when searching for non-KW annotations
    idx = kdb.filter_annotation(conn, source_type=("KW","%"), invert=True, taxonomy_id=2)
    assert idx == [3, 9, 17, 18, 19]

    # check that all three annotations (18,19,20) are returned when searching for non-(SRKW,S01) annotations
    idx = kdb.filter_annotation(conn, source_type=("SRKW","S01"), invert=True, taxonomy_id=2)
    assert idx == [3, 9, 13, 14, 17, 18, 19, 20]

    # check that the first two annotations (18,19) are returned when searching for non-(SRKW,S02) annotations
    idx = kdb.filter_annotation(conn, source_type=("SRKW","S02"), invert=True, taxonomy_id=2)
    assert idx == [2, 3, 8, 9, 12, 14, 17, 18, 19]

    # check that second and third annotations (19,20) is returned when searching for non-HW annotations
    idx = kdb.filter_annotation(conn, source_type=("HW","%"), invert=True, taxonomy_id=2)
    assert idx == [1, 2, 3, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 19, 20]


    # check that `exclude` arg works as intended when filtering
    # expecting query to return annotations 13, 14, and 20
    idx = kdb.filter_annotation(conn, source_type=("KW","%"), exclude=("SRKW", "S01"), taxonomy_id=2)
    assert idx == [13, 14, 20]


def test_import_taxonomy(basic_db):
    """ Test import_taxonomy function"""
    (conn, src_path) = basic_db
    conn.close()

    dst_path = os.path.join(path_to_assets, "db-test-import.sqlite")
    if os.path.exists(dst_path):
        os.remove(dst_path)

    conn = kdb.create_db(dst_path)

    c = kdb.import_taxonomy(conn, src_path, name="SimpleTax", new_name="NewName")

    c = conn.cursor()
    rows = c.execute("SELECT name,version FROM taxonomy").fetchall()
    assert len(rows) == 4
    for i,r in enumerate(rows):
        assert r[0] == "NewName"
        assert r[1] == i + 1

    os.remove(dst_path)


def test_custom_column(basic_db, deploy_data):
    """ Test that we can add a custom column to a table"""
    (conn, path) = basic_db

    conn.execute("ALTER TABLE deployment ADD COLUMN cost REAL")

    deploy_data["cost"] = 30

    c = kdb.insert_row(conn, table_name="deployment", values=deploy_data)

    rows = c.execute("SELECT cost FROM deployment").fetchall()
    assert rows[0][0] == 30

    conn.commit()