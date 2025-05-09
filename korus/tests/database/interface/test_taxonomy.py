import pytest
import pandas.testing
from copy import copy
from datetime import datetime, timezone
from korus.database.interface import TaxonomyInterface, LabelInterface
from korus.taxonomy import AcousticTaxonomy
from korus.tests.conftest import InMemoryTableBackend



def test_taxonomy_interface():
    """Check that TaxonomyInterface behaves as it should"""
    taxonomy_backend = InMemoryTableBackend()
    label_backend = InMemoryTableBackend()

    li = LabelInterface(label_backend)
    ti = TaxonomyInterface(taxonomy_backend, li)

    assert ti.version == 0
    assert len(ti) == 0
    assert isinstance(ti.draft, AcousticTaxonomy)

    # save the draft
    ti.save()

    # version is still 0, but table now has 1 entry
    assert ti.version == 0
    assert len(ti) == 1

    # make a release with no comment
    t = datetime.now(timezone.utc)
    ti.release()

    # version is now 1 and table has 2 entries
    assert ti.version == 1
    assert len(ti) == 2

    # release was timestamped correctly
    assert (ti.current.timestamp - t).total_seconds() < 0.01

    # make a release with a comment
    t = datetime.now(timezone.utc)
    ti.release("abc")

    assert ti.version == 2
    assert len(ti) == 3
    assert ti.current.comment == "abc"

    # save draft with comment
    ti.save("xyz")    
    assert ti.version == 2
    assert len(ti) == 3
    assert ti.current.comment == "abc"
    assert ti.draft.comment == "xyz"

    # check that we can reload the realeses and saved draft and that labels are correct
    labels = ti.labels.copy()
    ti.load()
    assert pandas.testing.assert_frame_equal(labels, ti.labels) is None
