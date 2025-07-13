import os
import pytest
from korus.taxonomy.acoustic import AcousticTaxonomy


file_dir = os.path.dirname(os.path.realpath(__file__))
path_to_assets = os.path.join(file_dir, "..", "assets")
path_to_tmp = os.path.join(path_to_assets, "tmp")


def test_last_common_ancestor(toy_acoustic_taxonomy):
    tax = toy_acoustic_taxonomy

    lca = tax.last_common_ancestor([("AA", "b3"), ("AB", "b4")])
    assert lca == ("A", "b")

    lca = tax.last_common_ancestor([("AB", "b4"), ("ABA", "a1")])
    assert lca == ("AB", "Unknown")

    with pytest.raises(AssertionError):
        tax.last_common_ancestor([("AA", "invalid-tag"), ("AB", "b4")])


def test_ascend_descend(toy_acoustic_taxonomy):
    tax = toy_acoustic_taxonomy

    tags = [tag for tag in tax.ascend("AB", "b4", include_start_node=False)]
    assert tags == [
        ("AB", "b"),
        ("AB", "Unknown"),
        ("A", "b4"),
        ("A", "b"),
        ("A", "Unknown"),
        ("Unknown", "Unknown"),
    ]

    tags = [tag for tag in tax.descend("AB", "a", include_start_node=False)]
    assert tags == [("ABA", "a"), ("ABA", "a1"), ("ABA", "a2")]

    tags = [tag for tag in tax.ascend("ABA", "*")]
    assert tags == [("ABA", "*"), ("AB", "*"), ("A", "*"), ("Unknown", "*")]

    tags = [tag for tag in tax.ascend("ABA", "*", include_start_node=False)]
    assert tags == [("AB", "*"), ("A", "*"), ("Unknown", "*")]

    tags = [tag for tag in tax.ascend("Unknown", "*", include_start_node=False)]
    assert tags == []

    tags = [tag for tag in tax.ascend("Unknown", "a", include_start_node=False)]
    assert tags == [("Unknown", "Unknown")]

    tags = [tag for tag in tax.descend("ABA", "a1", include_start_node=False)]
    assert tags == []

    tags = [tag for tag in tax.descend("ABA", "a", include_start_node=False)]
    assert tags == [("ABA", "a1"), ("ABA", "a2")]

    tags = [tag for tag in tax.descend("ABA", "a")]
    assert tags == [("ABA", "a"), ("ABA", "a1"), ("ABA", "a2")]

    tags = [tag for tag in tax.descend("ABA", "*", include_start_node=False)]
    assert tags == []

    tags = [tag for tag in tax.descend("A", "b")]
    assert tags == [
        ("A", "b"),
        ("A", "b1"),
        ("A", "b2"),
        ("A", "b3"),
        ("A", "b4"),
        ("AA", "b"),
        ("AA", "b1"),
        ("AA", "b2"),
        ("AA", "b3"),
        ("AB", "b"),
        ("AB", "b1"),
        ("AB", "b2"),
        ("AB", "b4"),
        ("ABA", "b"),
        ("ABA", "b1"),
        ("ABA", "b2"),
        ("ABA", "b4"),
    ]
