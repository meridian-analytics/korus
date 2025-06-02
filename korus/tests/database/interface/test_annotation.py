import pytest
from datetime import datetime
from korus.database.interface import (
    AnnotationInterface,
    JobInterface,
    TaxonomyInterface,
    LabelInterface,
    TagInterface,
    GranularityInterface,
)
from korus.tests.helpers import InMemoryTableBackend


def test_add_get_set_filter_data():
    """Check that we can add data to, retrieve data from, and modify data in an AnnotationInterface instance"""

    label = LabelInterface(InMemoryTableBackend())
    job = JobInterface(InMemoryTableBackend())
    tax = TaxonomyInterface(InMemoryTableBackend(), label)
    tag = TagInterface(InMemoryTableBackend())
    gran = GranularityInterface(InMemoryTableBackend())
    annot = AnnotationInterface(InMemoryTableBackend(), tax, job, tag, gran)

    # create a small taxonomy
    tax.draft.create_sound_source("Whale", parent="Unknown")
    tax.draft.create_sound_source("KW", parent="Whale")
    tax.draft.create_sound_source("SRKW", parent="KW")
    tax.draft.create_sound_type("TC", "Whale", "Unknown")
    tax.draft.create_sound_type("PC", "KW", "TC")
    tax.draft.create_sound_type("S01", "SRKW", "PC")
    tax.release()

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
    # assert idx == [1]
