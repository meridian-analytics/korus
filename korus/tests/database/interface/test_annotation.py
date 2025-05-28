import pytest
from datetime import datetime
from korus.database.interface import (
    AnnotationInterface,
    JobInterface,
    TaxonomyInterface,
    LabelInterface,
)
from korus.tests.conftest import InMemoryTableBackend


def test_add_get_set_data():
    """Check that we can add data to, retrieve data from, and modify data in an AnnotationInterface instance"""

    jb = InMemoryTableBackend()
    tb = InMemoryTableBackend()
    ab = InMemoryTableBackend()
    lb = InMemoryTableBackend()

    li = LabelInterface(lb)
    ji = JobInterface(jb)
    ti = TaxonomyInterface(tb, li)
    ai = AnnotationInterface(ab, ti, ji)

    # create a small taxonomy
    ti.draft.create_sound_source("Whale", parent="Unknown")
    ti.draft.create_sound_source("KW", parent="Whale")
    ti.draft.create_sound_source("SRKW", parent="KW")
    ti.draft.create_sound_type("TC", "Whale", "Unknown")
    ti.draft.create_sound_type("PC", "KW", "TC")
    ti.draft.create_sound_type("S01", "SRKW", "PC")
    ti.release()

    # add a job
    ji.add({"taxonomy_id": 0})

    # add a couple of annotations
    row = {
        "deployment_id": 0,
        "job_id": 0,
        "label": ("SRKW", "S01"),
        "start_utc": datetime(2022, 1, 1),
        "duration": 3.0,
        "start": 17.1,
    }
    ai.add(row)
    row = {
        "deployment_id": 0,
        "job_id": 0,
        "label": ("KW", "PC"),
        "start_utc": datetime(2023, 1, 1),
        "duration": 2.0,
        "start": 18.1,
    }
    ai.add(row)

    # filter
    idx = ai.filter({"label": ("SRKW", "*")}).indices
    assert idx == [0]
