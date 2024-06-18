import os
import pytest
import korus.tax as kx
from korus.tree import Tree

current_dir = os.path.dirname(os.path.realpath(__file__))
path_to_assets = os.path.join(current_dir, "assets")
path_to_tmp = os.path.join(path_to_assets, "tmp")


def test_taxonomy():
    """ Test basic functionalities of the Taxonomy class. Not a comprehensive test."""
    sqlite_path = os.path.join(path_to_tmp, "mytax.sqlite")
    if os.path.exists(sqlite_path):
        os.remove(sqlite_path)

    # check that we can initialize the class
    tax = kx.Taxonomy(name="mytax", root_tag="base", path=sqlite_path)

    # check that we can create some nodes
    tax.create_node("PC", parent="base", name="Pulsed Call")
    tax.create_node("BZ", parent="base")
    tax.create_node("S1", parent="PC")
    tax.create_node("S2", parent="PC", name="an S2 call")
    tax.create_node("S3", parent="PC")
    tax.create_node("S4", parent="PC")
    tax.create_node("S5", parent="PC")

    n = tax.get_node("S2")
    assert n.tag == "S2"
    assert n.data["name"] == "an S2 call"

    # if we attempt to merge two 'new' nodes, Korus complains
    with pytest.raises(RuntimeError):
        tax.merge_nodes("S23", children=["S2","S3"])

    # check that we are keeping track of created and deleted nodes
    assert len(tax.removed_nodes) == 0
    assert len(tax.created_nodes) == 8 #7 + 1 root node

    count = 0
    for k,v in tax.created_nodes.items():
        # check that root node has created_from = None and is_equivalent = False
        if count == 0:
            assert v[0] == [None]
            assert not v[1]

        # check that the next two nodes have created_from = root note and is_equivalent = False
        elif count == 1 or count == 2:
            assert v[0] == [tax.get_id("base")]
            assert v[1] == False

        # check that the remaining nodes have created_from = PC and is_equivalent = False
        elif count > 2:
            assert v[0] == [tax.get_id("PC")]
            assert v[1] == False

        count += 1

    # check that we are able to save the taxonomy and that it is assigned version no. 1
    tax.save()
    assert tax.latest_version() == 1

    # check we can now merge nodes
    tax.merge_nodes("S23", children=["S2","S3"])
    tax.merge_nodes("S45", children=["S4","S5"], remove=True)

    # check that we are keeping correct track of created/removed nodes
    assert len(tax.created_nodes) == 2
    assert len(tax.removed_nodes) == 2

    for k,v in tax.created_nodes.items():
        n = tax.get_node(k)

        if n.tag == "S23":
            assert len(v[0]) == 2
            assert v[0] == [tax.get_id("S2"), tax.get_id("S3")]
            assert v[1] == True

        elif n.tag == "S45":
            assert len(v[0]) == 2
            assert v[1] == True

    for k,v in tax.removed_nodes.items():
        assert v[0] == [tax.get_id("S45")]
        assert v[1] == False

    assert tax.version == 1

    tax.save()
    assert tax.latest_version() == 2

    #check that we can load the taxonomy back into memory
    tax = kx.Taxonomy.load(sqlite_path, name="mytax", version=2)
    assert tax.version == 2
    assert tax.path == sqlite_path


def test_acoustic_taxonomy():
    """ Test basic functionalities of the AcousticTaxonomy class. Not a comprehensive test."""
    sqlite_path = os.path.join(path_to_tmp, "myacoutax.sqlite")
    if os.path.exists(sqlite_path):
        os.remove(sqlite_path)

    # check that we can initialize the class
    tax = kx.AcousticTaxonomy(name="myacoutax", path=sqlite_path)

    # check that we can create some sound sources
    tax.create_sound_source("Bio", name="Biological sound source")
    tax.create_sound_source("Mammal", parent="Bio", name="Any mammal")
    kw = tax.create_sound_source("KW", parent="Mammal")
    kw = tax.create_sound_source("Dolphin", parent="Mammal")
    kw = tax.create_sound_source("HW", parent="Mammal")

    n = tax.get_node("HW")
    assert n.tag == "HW"
    assert tax.parent(tax.get_id("HW")).tag == "Mammal"

    # and some sound types
    tax.create_sound_type("TC", "Mammal", name="Tonal Call")
    tax.create_sound_type("CK", "Dolphin", name="Click")
    tax.create_sound_type("CK", "KW", name="Click")
    tax.create_sound_type("PC", "KW", name="Pulsed Call")

    t = tax.sound_types("KW")
    n = t.get_node("PC")
    assert n.tag == "PC"
    assert n.data["name"] == "Pulsed Call"
    assert t.parent(t.get_id("PC")).tag == "Unknown"

    # add another source, after adding sound types
    srkw = tax.create_sound_source("SRKW", parent="KW")

    # check that it inherited the sound types
    t_kw = tax.sound_types("KW")
    t_srkw = tax.sound_types("SRKW")
    for nid in t_kw.expand_tree(mode=Tree.DEPTH):
        n_srkw = t_srkw.get_node(nid)
        assert n_srkw.identifier == nid

    # add more sound types
    tax.create_sound_type("S01", "SRKW", parent="PC", name="S1 call")
    tax.create_sound_type("S02", "SRKW", parent="PC", name="S2 call")
    tax.create_sound_type("S03", "SRKW", parent="PC", name="S3 call")
    tax.create_sound_type("TCX", "SRKW", parent="TC", name="Tonal call X")

    # check that we can save the taxonomy
    tax.save()
    assert tax.latest_version() == 1

    # check that we can merge sound sources
    tax.merge_sound_sources(
        "Toothed", 
        children=["KW","Dolphin"], 
        remove=False, 
        data_merge_fcn=None,
        name="Odontecetes"
    )

    # check that toothed whales inherit sound types from parent node (Mammal)
    t_mammal = tax.sound_types("Mammal")
    t_toothed = tax.sound_types("Toothed")
    for nid in t_mammal.expand_tree(mode=Tree.DEPTH):
        n_toothed = t_toothed.get_node(nid)
        assert n_toothed.identifier == nid

    # check that we can merge sound types
    tax.merge_sound_types(
        tag="S2&3",
        source_tag="SRKW", 
        children=["S02","S03"], 
        remove=False, 
        data_merge_fcn=None,
    )

    # save version 2
    tax.save()
    assert tax.latest_version() == 2

    # check that we can load the taxonomy back into memory
    tax = kx.AcousticTaxonomy.load(sqlite_path, name="myacoutax", version=1)
    assert tax.version == 1

    # verify that sound types are as expected
    sound_types = tax.sound_types("SRKW")
    expected = ["Unknown","CK","PC","S01","S02","S03","TC","TCX"]
    for i,nid in enumerate(sound_types.expand_tree(mode=Tree.DEPTH)):
        assert sound_types.get_node(nid).tag == expected[i]

    # check that we can ascend up through the sound-type tree
    gen = tax.ascend("SRKW", "S01", include_start_node=False)
    expected = [
        ("SRKW","PC"),
        ("SRKW","Unknown"),
        ("KW","PC"),
        ("KW","Unknown"),
        ("Mammal","Unknown"),
        ("Bio","Unknown"),
        ("Unknown","Unknown"),      
    ]
    for i,(s,t) in enumerate(gen):
        assert (s, t) == expected[i]

    gen = tax.ascend("SRKW", include_start_node=False)
    expected = ["KW","Mammal","Bio","Unknown"]
    for i,(s,t) in enumerate(gen):
        assert s == expected[i]

    # check that we can descend down through the sound-type tree
    gen = tax.descend("Mammal", "TC", include_start_node=True)
    expected = [
        ("Mammal","TC"),
        ("Dolphin","TC"),
        ("HW","TC"),
        ("KW","TC"),
        ("SRKW","TC"),
        ("SRKW","TCX"),      
    ]
    for i,(s,t) in enumerate(gen):
        assert (s, t) == expected[i]

    gen = tax.descend("KW", include_start_node=False)
    expected = ["SRKW"]
    for i,(s,t) in enumerate(gen):
        assert s == expected[i]


    tax.create_sound_type("S99", "SRKW", parent="PC", name="S99 call")
    tax.save("added S99 call")

    tax.create_sound_type("S100", "SRKW", parent="PC", name="S100 call")
    tax.save("added S100 call", overwrite=True)

    tax.create_sound_type("S101", "SRKW", parent="PC", name="S101 call")
    tax.save("added S101 call")

    tax.create_sound_type("S102", "SRKW", parent="PC", name="S102 call")
    tax.save("added S102 call")

    tax = kx.AcousticTaxonomy.load(sqlite_path, name="myacoutax", version=4)
    tax.create_sound_type("S103", "SRKW", parent="PC", name="S103 call")
    tax.save("added S103 call", overwrite=True)

    import sqlite3
    conn = sqlite3.connect(sqlite_path)
    c = conn.cursor()
    rows = c.execute("SELECT id,name,version,comment FROM taxonomy").fetchall()
    rows[0] == (1, 'myacoutax', 1, None)
    rows[1] == (2, 'myacoutax', 2, None)
    rows[2] == (3, 'myacoutax', 3, 'added S99 call; added S100 call')
    rows[3] == (4, 'myacoutax', 4, 'added S101 call; added S103 call')
    rows[4] == (5, 'myacoutax', 5, 'added S102 call')


    rows = c.execute(f"SELECT * FROM taxonomy_created_node WHERE taxonomy_id = {4}").fetchall()
    print(rows)

    conn.close()