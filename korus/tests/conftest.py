import os
import pytest
import json
import pandas as pd
from datetime import datetime, timedelta
import korus.tax as kx
import korus.db as kdb

path_to_assets = os.path.join(os.path.dirname(__file__), "assets")
path_to_tmp = os.path.join(path_to_assets, "tmp")


@pytest.fixture
def basic_db():
    sqlite_path = os.path.join(path_to_tmp, "basic_db.sqlite")
    if os.path.exists(sqlite_path):
        os.remove(sqlite_path)

    conn = kdb.create_db(sqlite_path)

    # create a fairly simple acoustic taxonomy
    tax = kx.AcousticTaxonomy(name="SimpleTax", path=sqlite_path)

    # add sources
    tax.create_sound_source("Bio", name="Biological sound source")
    tax.create_sound_source("Mammal", parent="Bio", name="Any mammal")
    kw = tax.create_sound_source("KW", parent="Mammal")
    kw = tax.create_sound_source("Dolphin", parent="Mammal")
    kw = tax.create_sound_source("HW", parent="Mammal")

    # add sound types
    tax.create_sound_type("TC", "Mammal", name="Tonal Call")
    tax.create_sound_type("CK", "Dolphin", name="Click")
    tax.create_sound_type("CK", "KW", name="Click")
    tax.create_sound_type("PC", "KW", name="Pulsed Call")
    tax.create_sound_type("W", "KW", name="Whistle")

    # save, version no. 1
    tax.save(comment="this is the first version")
    #print(tax)

    # add another source and some more sound types
    srkw = tax.create_sound_source("SRKW", parent="KW")
    tax.create_sound_type("S01", "SRKW", parent="PC", name="S1 call")
    tax.create_sound_type("S02", "SRKW", parent="PC", name="S2 call")

    srkw = tax.create_sound_source("NRKW", parent="KW")
    tax.create_sound_type("N01", "NRKW", parent="PC", name="N1 call")

    tax.save("added SRKW and NRKW") # version no. 2
    #print(tax)

    # remove NRKW again
    tax.remove_node("NRKW")
    tax.save("removed NRKW") # version no. 3
    #print(tax)

    # link past KW
    tax.link_past_node("KW")
    tax.save("linked past KW") # version no. 4
    #print(tax)

    yield conn, sqlite_path

    conn.close()
    if os.path.exists(sqlite_path):
        os.remove(sqlite_path)


@pytest.fixture
def deploy_data():
    lat = 49.780487
    lon = -122.05154
    depth = 18.0
    v = {
        "owner": "OceanResearch",
        "name": "WestPoint",
        "start_utc": "2022-06-24",
        "end_utc": "2022-10-03",
        "location": "Salt Spring Island, BC, Canada",
        "latitude_deg": lat,
        "longitude_deg": lon,
        "depth_m": depth,
        "latitude_min_deg": lat,
        "latitude_max_deg": lat,
        "longitude_min_deg": lon,
        "longitude_max_deg": lon,
        "depth_min_m": depth,
        "depth_max_m": depth,
        "license": None,
        "hydrophone": None,
        "bits_per_sample": None,
        "sample_rate": 128000,
        "num_channels": 1,
        "sensitivity": None,
        "comments": None
    }
    return v


@pytest.fixture
def file_data():

    def abclisten_timestamp_parser(x):
        fmt = "_%Y%m%dT%H%M%S.%fZ"
        p = x.find("_")
        s = x[p: p + 21]
        return datetime.strptime(s, fmt)

    fnames =[
        "ABCLISTENHF1234_20220624T164000.000Z_20220624T164459.996Z.flac",
        "ABCLISTENHF1234_20220624T164500.023Z_20220624T164959.994Z.flac"
    ]

    deploy_path = "OceanResearch/WestPoint"

    file_data = []
    for fname in fnames:
        dt = abclisten_timestamp_parser(fname)
        dir_path = os.path.join(deploy_path, dt.strftime("%Y%m%d"))
        num_samples, sample_rate = 32000*5*60, 32000
        start_utc_str = dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        end_utc_str = (dt+timedelta(seconds=num_samples/sample_rate)).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        v = {
            "deployment_id": 1,
            "storage_id": 1,
            "filename": fname,
            "relative_path": dir_path,
            "sample_rate": sample_rate,
            "downsample": None,
            "num_samples": num_samples,
            "format": "FLAC",
            "codec": "FLAC",
            "start_utc": start_utc_str,
            "end_utc": end_utc_str,
        }
        file_data.append(v)

    return file_data


@pytest.fixture
def db_with_annotations(basic_db, deploy_data, file_data):
    """ Creates a database with a small set of annotations.

        Database contents:

            * A single hydrophone deployment
            * Two, consecutive 5-minute audio files sampled at 32 kHz, with the first audio 
                file starting at 2022-06-24 16:40:00.000
            * An exhaustive annotation job targeting KW,PC and KW,W
            * Three annotations:
                - KW,PC: starting 30.0s into the first audio file and lasting 1.3s
                - SRKW,S01: starting 21.1s into the first audio file and lasting 5 minutes
                - None,None: a manual negative starting 1.0s into the first audio file and lasting 0.8s
                - auto-generated negatives
    """
    (conn, sqlite_path) = basic_db

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
        "model_config": None,
        "start_utc": "2022-10",
        "end_utc": "2023-03",
        "by_human": 1,
        "by_machine": 0,
        "comments": "Vessel noise annotated opportunistically"
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

    # define a tag
    v = {
        "name": "NEGATIVE",
        "description": "A negative sample"
    }
    c = kdb.insert_row(conn, table_name="tag", values=v)

    #insert annotation using add_annotation function
    annot_tbl = pd.DataFrame({
        "file_id": [1,1,1],
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
    neg_ids = kdb.add_negatives(conn, job_id=1)

    conn.commit()

    yield conn, sqlite_path

    conn.close()
    if os.path.exists(sqlite_path):
        os.remove(sqlite_path)
