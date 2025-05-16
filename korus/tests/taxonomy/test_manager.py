import os
import pytest
from korus.taxonomy.manager import LabelManager, get_label_id
from korus.taxonomy.taxonomy import Taxonomy


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
    m.update(1, rows)

    # passing an invalid version no. triggers error
    with pytest.raises(ValueError):
        m.get_label_id((0, "A"))

    # null label as ID=0
    id = m.get_label_id((1, None))
    assert id == 0

    # labels are assigned integer IDs in the order they are added
    id = m.get_label_id((1, "A"))
    assert id == 1

    id = m.get_label_id((1, "B"))
    assert id == 2

    # we can use wildcards
    id = m.get_label_id((1, "*"))
    assert id == [0, 1, 2]

    # add another label
    rows += [("C", 103)]
    m.update(2, rows)
    id = m.get_label_id(("*", "A"))
    assert id == [1, 4]


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
