import os
import pytest
import pandas as pd
import numpy as np
import korus.db as kdb
import korus.app.app_util.add as add


current_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
path_to_assets = os.path.join(current_dir, "assets")
path_to_tmp = os.path.join(path_to_assets, "tmp")


def test_from_raven(basic_db):
    (conn, path) = basic_db
    tax = kdb.get_taxonomy(conn, taxonomy_id=2)
    granularity = "window"
    path = os.path.join(path_to_assets, "raven-sel-table-1.txt")
    df = add.from_raven(path, tax, granularity, interactive=False)
    
    # check 1st entry
    r = df.iloc[0]
    assert r.valid == 0
    assert r.sound_source == "SRKW"
    assert r.sound_type == "PC/W"
    assert r.tentative_sound_source == None
    assert r.tentative_sound_type == None
    assert r.ambiguous_sound_source == None
    assert r.ambiguous_sound_type == ["S01","S02"]
    assert r.granularity == "window"

    # check 2nd entry
    r = df.iloc[1]
    assert r.valid == 1
    assert r.sound_source == "SRKW"
    assert r.sound_type == "PC"
    assert r.tentative_sound_source == None
    assert r.tentative_sound_type == None
    assert r.ambiguous_sound_source == None
    assert r.ambiguous_sound_type == ["S01","S02"]
    assert r.granularity == "window"

    # check 3rd entry
    r = df.iloc[2]
    assert r.valid == 0
    assert r.sound_source == "SRKW"
    assert r.sound_type == "PCX"
    assert r.tentative_sound_source == None
    assert r.tentative_sound_type == None
    assert r.ambiguous_sound_source == None
    assert r.ambiguous_sound_type == None
    assert r.granularity == "unit"

    # check 4th entry
    r = df.iloc[3]
    assert r.valid == 1
    assert r.sound_source == "KW"
    assert r.sound_type == "PC"
    assert r.tentative_sound_source == "SRKW"
    assert r.tentative_sound_type == "S01"
    assert r.ambiguous_sound_source == None
    assert r.ambiguous_sound_type == None
    assert r.granularity == "unit"


def test_print_summary(db_with_annotations):
    conn, sqlite_path = db_with_annotations
    df = kdb.get_annotations(conn)
    add.print_annotation_summary(conn)