import os
import pytest
import korus.tree as ktr

current_dir = os.path.dirname(os.path.realpath(__file__))
path_to_assets = os.path.join(current_dir, "assets")
path_to_tmp = os.path.join(path_to_assets, "tmp")


def test_ktree():
    """ A rather basic, not very comprehensive test of the KTree class"""
    # check that we can initialize the class
    t = ktr.KTree(root_tag="base")

    # check that we can create some nodes
    t.create_node("PC", parent="base", name="Pulsed Call")
    t.create_node("BZ", parent="base")
    t.create_node("S1", parent="PC")
    t.create_node("S2", parent="PC")
    t.create_node("S3", parent="PC")
    t.create_node("S4", parent="PC")
    n = t.create_node("S5", parent="PC", name="S5 call")

    assert n.tag == "S5"
    assert n.data["name"] == "S5 call"
    assert t.parent(t.get_id("S5")).tag == "PC"

    # check that an Error is raised if we attempt to merge two 'new' nodes
    with pytest.raises(RuntimeError):
        t.merge_nodes("S23", children=["S2","S3"])

    # check that we can merge nodes after clearing history
    t.clear_history()
    t.merge_nodes("S23", children=["S2","S3"])
    n = t.get_node("S23")
    assert n.tag == "S23"
    assert t.parent(t.get_id("S23")).tag == "PC"
    assert t.parent(t.get_id("S2")).tag == "S23"

    # check that if remove=True, the children are removed after merge
    t.merge_nodes("S45", children=["S4","S5"], remove=True)
    n = t.get_node("S45")
    assert n.tag == "S45"
    assert t.parent(t.get_id("S45")).tag == "PC"
    with pytest.raises(AttributeError):
        t.get_node("S4").tag

    # test that last_common_ancestor method works as intended
    assert t.last_common_ancestor(["S2","S3"]) == "S23"
    assert t.last_common_ancestor(["S2","S1"]) == "PC"
    assert t.last_common_ancestor(["S2","S23"]) == "S23"
    assert t.last_common_ancestor(["S45","PC"]) == "PC"
    assert t.last_common_ancestor(["S45","BZ"]) == "base"
