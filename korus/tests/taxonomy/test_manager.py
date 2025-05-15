import os
import pytest
from korus.taxonomy.manager import LabelManager


file_dir = os.path.dirname(os.path.realpath(__file__))
path_to_assets = os.path.join(file_dir, "..", "assets")
path_to_tmp = os.path.join(path_to_assets, "tmp")


def test_label_manager():
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
