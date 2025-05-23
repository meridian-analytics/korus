import os
import pytest
from korus.taxonomy.manager import TaxonomyManager, LabelManager, get_label_id
from korus.taxonomy.taxonomy import Taxonomy


file_dir = os.path.dirname(os.path.realpath(__file__))
path_to_assets = os.path.join(file_dir, "..", "assets")
path_to_tmp = os.path.join(path_to_assets, "tmp")


def test_trace_node_history():
    lm = LabelManager()
    tax = Taxonomy()
    m = TaxonomyManager(tax, lm)
    m.draft.create_node("A", parent="root")
    m.release()  # 1st release
    m.draft.create_node("A-A", parent="A")
    m.draft.create_node("A-B", parent="A")
    m.release()  # 2nd release
    m.draft.remove_node("A-A")
    m.draft.create_node("A-C", parent="A")
    m.release()  # 3rd release
    m.draft.merge_nodes("A-BC", ["A-B", "A-C"])
    m.release()  # 4th release
    m.draft.remove_node("A-BC")
    m.release()  # 5th release

    # check that merged node maps to itself within the same version
    nid_ABC = m.releases[3].get_node("A-BC").identifier
    nids, equiv = m.trace_node_history(nid_ABC, 4)
    assert nids == [nid_ABC]
    assert equiv

    # check that merged node maps to its precursors
    nids, equiv = m.trace_node_history(nid_ABC, 3)
    nid_AB = m.releases[2].get_node("A-B").identifier
    nid_AC = m.releases[2].get_node("A-C").identifier
    assert nids == [nid_AB, nid_AC]
    assert equiv

    # check that merged node maps to its inheritors
    nids, equiv = m.trace_node_history(nid_ABC, 5, mode="f")
    nid_A = m.releases[1].get_node("A").identifier
    assert nids == [nid_A]
    assert not equiv

    # check that removed node maps to its inheritors
    nid_AA = m.releases[1].get_node("A-A").identifier
    nids, equiv = m.trace_node_history(nid_AA, 3, mode="f")
    nid_A = m.releases[1].get_node("A").identifier
    assert nids == [nid_A]
    assert not equiv


def test_get_label_id():
    """Test `get_label_id` function"""
    tax = Taxonomy(version=1)
    tax.create_node("A", parent="root")
    tax.create_node("AA", parent="A")
    tax.create_node("AB", parent="A")
    tax.create_node("ABA", parent="AB")

    m = LabelManager()

    m.update(1, tax.all_labels)

    id = get_label_id("AA", tax, m)
    assert id == 3

    id = get_label_id("AA", tax, m, ascend=True)
    assert id == [3, 2, 1]

    id = get_label_id("AB", tax, m, descend=True)
    assert id == [4, 5]

    id = get_label_id("AB", tax, m, ascend=True, descend=True)
    assert sorted(id) == sorted([1, 2, 4, 5])


def test_taxonomy_manager():
    lm = LabelManager()

    tax = Taxonomy()

    m = TaxonomyManager(tax, lm)

    m.draft.create_node("A", parent="root")
    m.draft.create_node("AA", parent="A")
    m.draft.create_node("AB", parent="A")
    m.draft.create_node("ABA", parent="AB")

    m.release()

    id = m.get_label_id("AA")
    assert id == 3

    id = m.get_label_id("AA", 1, ascend=True)
    assert id == [3, 2, 1]

    id = m.get_label_id("AB", descend=True)
    assert id == [4, 5]

    id = m.get_label_id("AB", ascend=True, descend=True)
    assert sorted(id) == sorted([1, 2, 4, 5])

    id = m.get_label_id(label_id=4, descend=True)
    assert id == [4, 5]
