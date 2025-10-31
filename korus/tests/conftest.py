import os
import pytest
import pandas as pd
from datetime import datetime
from korus.database.backend.sqlite import SQLiteBackend
from korus.database import SQLiteDatabase
from korus.tests.helpers import InMemoryTableBackend, InMemoryJobBackend
from korus.database.interface import (
    AnnotationInterface,
    FileInterface,
    JobInterface,
    TaxonomyInterface,
    LabelInterface,
    TagInterface,
    GranularityInterface,
    StorageInterface,
)
from korus.taxonomy.acoustic import AcousticTaxonomy

path_to_assets = os.path.join(os.path.dirname(__file__), "assets")
path_to_tmp = os.path.join(path_to_assets, "tmp")

# ensure tmp directory exists
if not os.path.exists(path_to_tmp):
    os.makedirs(path_to_tmp)


@pytest.fixture
def toy_acoustic_taxonomy():
    """A toy acoustic taxonomy"""
    tax = AcousticTaxonomy()

    tax.create_sound_source("A", parent="Unknown")
    tax.create_sound_source("AA", parent="A")
    tax.create_sound_source("AB", parent="A")
    tax.create_sound_source("ABA", parent="AB")

    tax.create_sound_type("a", "Unknown")
    tax.create_sound_type("b", "A")
    tax.create_sound_type("b1", "A", parent="b")
    tax.create_sound_type("b2", "A", parent="b")
    tax.create_sound_type("b3", "A", parent="b", recursive=False)
    tax.create_sound_type("b4", "A", parent="b", recursive=False)
    tax.create_sound_type("b3", "AA", parent="b")
    tax.create_sound_type("b4", "AB", parent="b")
    tax.create_sound_type("c", "ABA")
    tax.create_sound_type("a1", "ABA", parent="a")
    tax.create_sound_type("a2", "ABA", parent="a")

    yield tax


@pytest.fixture
def in_memory_table_backend():
    """Instance of TableBackend that stores data in memory"""
    yield InMemoryTableBackend()


@pytest.fixture
def interfaces_with_taxonomy():
    """Yields a dict of table interfaces with a small, but realistic taxonomy"""
    label = LabelInterface(InMemoryTableBackend())
    storage = StorageInterface(InMemoryTableBackend())
    file = FileInterface(InMemoryTableBackend(), storage)
    job = JobInterface(InMemoryJobBackend(), file)
    tax = TaxonomyInterface(InMemoryTableBackend(), label)
    tag = TagInterface(InMemoryTableBackend())
    gran = GranularityInterface(InMemoryTableBackend())
    annot = AnnotationInterface(InMemoryTableBackend(), tax, job, file, tag, gran)

    # create a small taxonomy
    tax.draft.create_sound_source("Whale", parent="Unknown")
    tax.draft.create_sound_source("KW", parent="Whale")
    tax.draft.create_sound_source("SRKW", parent="KW")
    tax.draft.create_sound_type("TC", "Whale", "Unknown")
    tax.draft.create_sound_type("PC", "KW", "TC")
    tax.draft.create_sound_type("S01", "SRKW", "PC")
    tax.draft.create_sound_source("HW", parent="Whale")
    tax.release()

    yield {
        "file": file,
        "job": job,
        "taxonomy": tax,
        "tag": tag,
        "granularity": gran,
        "annotation": annot,
    }


@pytest.fixture
def job_interface_with_data():
    """Yields a JobInterface with two jobs and two files"""

    storage = StorageInterface(InMemoryTableBackend())
    file = FileInterface(InMemoryTableBackend(), storage)
    job = JobInterface(InMemoryJobBackend(), file)

    # add two jobs
    job.add({"taxonomy_id": 0})
    job.add({"taxonomy_id": 0})

    # add two files
    file.add(
        {
            "deployment_id": 0,
            "storage_id": 0,
            "filename": "xyz.flac",
            "sample_rate": 2000,
            "num_samples": 20000,
        }
    )
    file.add(
        {
            "deployment_id": 0,
            "storage_id": 0,
            "filename": "abc.wav",
            "sample_rate": 4000,
            "num_samples": 40000,
        }
    )

    # link 1st file (channel 0) to the 2nd job
    job.add_file(1, 0)

    # link 2nd file (channel 14) to both jobs
    job.add_file(0, 1, 14)
    job.add_file(1, 1, 14)

    yield job


@pytest.fixture
def minimal_sqlite_backend():
    """Yields an SQLite database backend with every table populated with a single entry
    where only the required fields have non-null values.
    """
    path = os.path.join(path_to_tmp, "test.sqlite")
    if os.path.exists(path):
        os.remove(path)

    backend = SQLiteBackend(path, new=True)

    backend.deployment.add({"name": "MyDeployment"})
    backend.storage.add({"name": "MyFileStorage"})
    backend.file.add(
        {
            "deployment_id": 0,
            "storage_id": 0,
            "filename": "abc.wav",
            "sample_rate": 96000,
            "num_samples": 960000,
        }
    )
    backend.taxonomy.add({"name": "MyTaxonomy", "tree": dict()})
    backend.job.add({"taxonomy_id": 0})
    backend.annotation.add({"deployment_id": 0, "job_id": 0})

    yield backend

    backend.close()
    if os.path.exists(path):
        os.remove(path)


@pytest.fixture
def sqlite_database_with_taxonomy():
    """Yields a Korus database with SQLite backend and a small, but realistic taxonomy."""
    path = os.path.join(path_to_tmp, "db_with_tax.sqlite")
    if os.path.exists(path):
        os.remove(path)

    db = SQLiteDatabase(path, new=True)

    # create a fairly simple acoustic taxonomy
    tax = db.taxonomy.draft

    # add sources
    tax.create_sound_source("Bio", name="Biological sound source")
    tax.create_sound_source("Mammal", parent="Bio", name="Any mammal")
    tax.create_sound_source("KW", parent="Mammal")
    tax.create_sound_source("Dolphin", parent="Mammal")
    tax.create_sound_source("HW", parent="Mammal")

    # add sound types
    tax.create_sound_type("TC", "Mammal", name="Tonal Call")
    tax.create_sound_type("CK", "Dolphin", name="Click")
    tax.create_sound_type("CK", "KW", name="Click")
    tax.create_sound_type("PC", "KW", parent="TC", name="Pulsed Call")
    tax.create_sound_type("W", "KW", parent="TC", name="Whistle")

    # release version 1
    db.taxonomy.release(comment="this is the first version")

    # add another source and some more sound types
    tax.create_sound_source("SRKW", parent="KW")
    tax.create_sound_type("S01", "SRKW", parent="PC", name="S1 call")
    tax.create_sound_type("S02", "SRKW", parent="PC", name="S2 call")
    tax.create_sound_source("NRKW", parent="KW")
    tax.create_sound_type("N01", "NRKW", parent="PC", name="N1 call")

    # version 2
    db.taxonomy.release("added SRKW and NRKW")

    # remove NRKW again
    tax.remove_node("NRKW")

    # version 3
    db.taxonomy.release("removed NRKW")

    # link past KW
    tax.link_past_node("KW")

    # version 4
    db.taxonomy.release("linked past KW")

    yield db

    db.backend.close()
    if os.path.exists(path):
        os.remove(path)


@pytest.fixture
def one_storage_location():
    v = {
        "name": "laptop",
        "path": "/",
    }
    return v


@pytest.fixture
def one_deployment():
    lat = 49.780487
    lon = -122.05154
    depth = 18.0
    v = {
        "name": "WestPoint",
        "start_utc": datetime(2022, 6, 24),
        "end_utc": datetime(2022, 10, 3),
        "latitude_deg": lat,
        "longitude_deg": lon,
        "depth_m": depth,
    }
    return v


@pytest.fixture
def two_files():
    """Two, consecutive 5-minute audio files sampled at 32 kHz, with the first audio file starting at 2022-06-24 16:40:00.000"""

    def abclisten_timestamp_parser(x):
        fmt = "_%Y%m%dT%H%M%S.%fZ"
        p = x.find("_")
        s = x[p : p + 21]
        return datetime.strptime(s, fmt)

    fnames = [
        "ABCLISTENHF1234_20220624T164000.000Z_20220624T164459.996Z.flac",
        "ABCLISTENHF1234_20220624T164500.023Z_20220624T164959.994Z.flac",
    ]

    dirpath = "OceanResearch/WestPoint"

    file_data = []
    for fname in fnames:
        dt = abclisten_timestamp_parser(fname)
        dir_path = os.path.join(dirpath, dt.strftime("%Y%m%d"))
        num_samples, sample_rate = 32000 * 5 * 60, 32000
        v = {
            "deployment_id": 0,
            "storage_id": 0,
            "filename": fname,
            "relative_path": dir_path,
            "sample_rate": sample_rate,
            "num_samples": num_samples,
            "start_utc": dt,
        }
        file_data.append(v)

    return file_data


@pytest.fixture
def one_job():
    """An exhaustive annotation job targeting KW,PC and KW,W"""
    target = [("KW", "PC"), ("KW", "W")]
    job_data = {
        "taxonomy_id": 2,
        "annotator": "LL",
        "target": target,
        "is_exhaustive": True,
        "completion_date": datetime(2023, 3, 1),
    }

    return job_data


@pytest.fixture
def three_annotations():
    """Three annotations:
    - KW,PC: starting 30.0s into the first audio file and lasting 1.3s
    - SRKW,S01: starting 21.1s into the first audio file and lasting 5 minutes
    - None,None: a manual negative starting 1.0s into the first audio file and lasting 0.8s
    """
    annot_data = {
        "deployment_id": [0, 0, 0],
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
    return annot_data


@pytest.fixture
def sqlite_database_with_some_data(
    sqlite_database_with_taxonomy,
    one_deployment,
    one_storage_location,
    one_job,
    two_files,
    three_annotations,
):
    db = sqlite_database_with_taxonomy

    db.deployment.add(one_deployment)
    db.storage.add(one_storage_location)
    for file in two_files:
        db.file.add(file)

    db.job.add(one_job)
    db.job.add_file(0, 0)
    db.job.add_file(0, 1)
    db.tag.add({"name": "NEGATIVE", "description": "a negative sample"})
    df = pd.DataFrame(three_annotations)
    for _, row in df.iterrows():
        db.annotation.add(row)

    yield db