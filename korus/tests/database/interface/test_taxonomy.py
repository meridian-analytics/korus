import os
import pytest
import pandas as pd
import pandas.testing
from datetime import datetime, timezone
from korus.database.interface import TaxonomyInterface, LabelInterface
from korus.taxonomy import AcousticTaxonomy
from korus.tests.helpers import InMemoryTableBackend
from korus.database import SQLiteDatabase


path_to_assets = os.path.join("korus", "tests", "assets")
path_to_tmp = os.path.join(path_to_assets, "tmp")


def test_taxonomy_interface():
    """Check that TaxonomyInterface behaves as it should with a super-simple, toy taxonomy"""
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


def test_get_label_id_acoustic_taxonomy(sqlite_database_with_taxonomy):
    """Test that we can use get_label_id function to retrieve correct label IDs for
    a small, but realistic acoustic taxonomy.
    """
    db = sqlite_database_with_taxonomy

    version = 2

    # fetch data from the `label` helper table for taxonomy version 2
    indices = db._label.filter({"taxonomy_id": 2}).indices
    data = db._label.get(
        indices=indices,
        fields=["sound_source_tag", "sound_type_tag"],
        return_indices=True,
    )
    # put fetched data in a DataFrame
    df = pd.DataFrame(
        {
            "id": [r[0] for r in data],
            "ss": [r[1] for r in data],
            "st": [r[2] for r in data],
        }
    )
    df.set_index("id", inplace=True)

    l = db.taxonomy.get_label_id(("SRKW", "S01"), version, ascend=False, descend=False)
    assert df.loc[l].ss == "SRKW"
    assert df.loc[l].st == "S01"

    l_list = db.taxonomy.get_label_id(
        ("SRKW", "*"), version, ascend=False, descend=False
    )
    for l in l_list:
        assert df.loc[l].ss == "SRKW"
        assert df.loc[l].st in ["S01", "S02", "PC", "CK", "W", "TC", "Unknown"]

    l_list = db.taxonomy.get_label_id(
        ("SRKW", "S01"), version, ascend=True, descend=False
    )
    for l in l_list:
        assert df.loc[l].ss in ["SRKW", "KW", "Mammal", "Bio", "Unknown"]
        assert df.loc[l].st in ["S01", "PC", "TC", "Unknown"]

    l_list = db.taxonomy.get_label_id(("KW", "PC"), version, ascend=False, descend=True)
    for l in l_list:
        assert df.loc[l].ss in ["KW", "SRKW", "NRKW"]
        assert df.loc[l].st in ["PC", "S01", "S02", "N01"]

    with pytest.raises(ValueError):
        db.taxonomy.get_label_id(("Fish", "PC"), version, ascend=False, descend=True)

    with pytest.raises(ValueError):
        db.taxonomy.get_label_id(("Fish", "*"), version, ascend=False, descend=True)

    with pytest.raises(ValueError):
        db.taxonomy.get_label_id(
            ("*", "LoudSound"), version, ascend=False, descend=True
        )


def test_load_taxonomy_from_sqlite():
    path = os.path.join(path_to_tmp, "db_with_tax.sqlite")

    if os.path.exists(path):
        os.remove(path)

    # create db and make 1 taxonomy release
    db = SQLiteDatabase(path, new=True)
    tax = db.taxonomy.draft
    tax.create_sound_source("Bio", name="Biological sound source")
    db.taxonomy.release()
    assert db.taxonomy.get_taxonomy(1).version == 1
    db.backend.close()

    # load db again, check version no
    db = SQLiteDatabase(path)
    assert db.taxonomy.get_taxonomy(1).version == 1
    db.backend.close()

    # cleanup
    if os.path.exists(path):
        os.remove(path)
