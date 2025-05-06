import pytest
from datetime import datetime, timezone
from korus.database.interface import TaxonomyInterface
from korus.taxonomy import AcousticTaxonomy


def test_taxonomy_interface(in_memory_table_backend):
    """Check that TaxonomyInterface behaves as it should"""
    ti = TaxonomyInterface(in_memory_table_backend)

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
