import os
import pytest
from korus.taxonomy.taxonomy import Taxonomy


file_dir = os.path.dirname(os.path.realpath(__file__))
path_to_assets = os.path.join(file_dir, "..", "assets")
path_to_tmp = os.path.join(path_to_assets, "tmp")


def test_ascend_descend():
    tax = Taxonomy()
    tax.create_node("A", parent="root")
    tax.create_node("AA", parent="A")
    tax.create_node("AB", parent="A")
    tax.create_node("ABA", parent="AB")

    tags = [tag for tag in tax.ascend("ABA")]
    assert tags == [("ABA",), ("AB",), ("A",), ("root",)]

    tags = [tag for tag in tax.ascend("ABA", False)]
    assert tags == [("AB",), ("A",), ("root",)]

    tags = [tag for tag in tax.ascend("AA", False)]
    assert tags == [("A",), ("root",)]

    tags = [tag for tag in tax.ascend("root", False)]
    assert tags == []

    tags = [tag for tag in tax.descend("root", False)]
    assert tags == [("A",), ("AA",), ("AB",), ("ABA",)]

    tags = [tag for tag in tax.descend("ABA", False)]
    assert tags == []
