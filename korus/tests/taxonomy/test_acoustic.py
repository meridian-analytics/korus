import os
import pytest
from korus.taxonomy.acoustic import AcousticTaxonomy


file_dir = os.path.dirname(os.path.realpath(__file__))
path_to_assets = os.path.join(file_dir, "..", "assets")
path_to_tmp = os.path.join(path_to_assets, "tmp")


def test_ascend_descend():
    tax = AcousticTaxonomy()

    tax.create_sound_source("A", parent="Unknown")
    tax.create_sound_source("AA", parent="A")
    tax.create_sound_source("AB", parent="A")
    tax.create_sound_source("ABA", parent="AB")

    tax.create_sound_type("a", "Unknown")
    tax.create_sound_type("b", "A")
    tax.create_sound_type("b1", "A", parent="b")
    tax.create_sound_type("b2", "A", parent="b")
    tax.create_sound_type("b3", "A", parent="b", recursive=False)
    tax.create_sound_type("b4", "A", parent="b", recursive=False)
    tax.create_sound_type("b3", "AA", parent="b")
    tax.create_sound_type("b4", "AB", parent="b")
    tax.create_sound_type("c", "ABA")
    tax.create_sound_type("a1", "ABA", parent="a")
    tax.create_sound_type("a2", "ABA", parent="a")

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
