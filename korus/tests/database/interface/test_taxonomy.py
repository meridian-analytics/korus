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

    # add a few sound sources and sound types to the taxonomy
    ti.draft.create_sound_source("A")
    ti.draft.create_sound_source("AA", parent="A")
    ti.draft.create_sound_type("a", "A")
    ti.draft.create_sound_type("aa", "A", parent="a")
    ti.draft.create_sound_type("b", "A")

    # make a release with no comment
    t = datetime.now(timezone.utc)
    ti.release()

    # version is now 1 and table has 2 entries
    assert ti.version == 1
    assert len(ti) == 2

    # release was timestamped correctly
    assert (ti.current.timestamp - t).total_seconds() < 0.01

    # add a new sound type to the draft
    # (note: gets added both to A and its child node, AA)
    ti.draft.create_sound_type("bb", "A", parent="b")

    # check that the draft has two more label pairs compared to the 1st release
    first_release_labels = set([(row[0], row[1]) for row in ti.releases[0].all_labels])
    draft_labels = set([(row[0], row[1]) for row in ti.draft.all_labels])
    diff = draft_labels - first_release_labels
    assert diff == {("AA", "bb"), ("A", "bb")}
    # print(diff)

    # make a release with a comment
    t = datetime.now(timezone.utc)
    ti.release("abc")

    assert ti.version == 2
    assert len(ti) == 3
    assert ti.current.comment == "abc"

    # capture current label ID map
    labels = ti.labels.df.copy()

    # add another node to the draft
    ti.draft.create_sound_type("c", "AA")

    # save draft with comment
    ti.save("xyz")
    assert ti.version == 2
    assert len(ti) == 3
    assert ti.current.comment == "abc"
    assert ti.draft.comment == "xyz"

    # check that we can reload the releases and saved draft
    ti.load()

    # check that label ID map is unchanged and doesn't include the
    # last node added to the draft, but not yet released
    assert pandas.testing.assert_frame_equal(labels, ti.labels.df) is None

    # check that the labels of the first release are correct
    first_release_labels_upon_load = set(
        [(row[0], row[1]) for row in ti.releases[0].all_labels]
    )
    diff = first_release_labels_upon_load - first_release_labels
    assert diff == set()
