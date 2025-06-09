import os
import pytest
from korus.taxonomy.manager import LabelManager


file_dir = os.path.dirname(os.path.realpath(__file__))
path_to_assets = os.path.join(file_dir, "..", "assets")
path_to_tmp = os.path.join(path_to_assets, "tmp")


def test_label_manager():
    """Basic tests for LabelManager class"""
    m = LabelManager()

    # add two labels
    rows = [
        ("A", 101),
        ("B", 102),
    ]
    m.update(1, rows)  # release 1

    # passing an invalid version no. triggers error
    with pytest.raises(ValueError):
        m.get_label_id(0, "A")

    # null label as ID=0
    # id = m.get_label_id(1, (None,))
    # assert id == 0

    # labels are assigned integer IDs in the order they are added
    id = m.get_label_id(1, "A")
    assert id == 0

    id = m.get_label_id(1, "B")
    assert id == 1

    # we can use wildcards
    id = m.get_label_id(1, "*")
    assert id == [0, 1]

    # add another label
    rows += [("C", 103)]
    m.update(2, rows)  # release 2

    # we can also use wildcards for release number
    id = m.get_label_id("*", "A")
    assert id == [0, 2]

    # get label ID using node IDs instead of tags
    id = m.get_label_id("*", "C")
    assert id == 4
    id = m.get_label_id("*", nid=103)
    assert id == 4

    # retrieve version and tags
    assert m.get_label(1) == (1, ("B",))
    assert m.get_label([0, 2]) == [(1, ("A",)), (2, ("A",))]


def test_label_manager_multiple_tags():
    """Tests for LabelManager class with multiple-tag labels"""
    m = LabelManager(tags=["tag1", "tag2"], nids=["id1", "id2"])

    # add two labels
    rows = [
        ("A", "a", 101, 102),
        ("B", "b", 201, 202),
    ]
    m.update(1, rows)

    id = m.get_label_id("*", ("A", "a"))
    assert id == 0

    id = m.get_label_id("*", nid=(101, 102))
    assert id == 0

    id = m.get_label_id("*", nid=(101, "*"))
    assert id == 0
