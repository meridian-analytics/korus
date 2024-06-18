import os
import pytest
from io import StringIO
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import korus.db_util.label as klb
import json

current_dir = os.path.dirname(os.path.realpath(__file__))
path_to_assets = os.path.join(current_dir, "assets")
path_to_tmp = os.path.join(path_to_assets, "tmp")


def test_crosswalk_label(basic_db):
    """Test that crosswalk_label function can be used to map labels across taxonomy versions"""
    (conn, path) = basic_db

    label_org = ("KW","PC")
    label_dst, is_equiv = klb.crosswalk_label(conn, label_org, origin_taxonomy_id=2, dst_taxonomy_id=1, always_list=True)
    assert label_dst == [('KW', 'PC')]
    assert is_equiv == True

    label_org = ("SRKW","S01")
    label_dst, is_equiv = klb.crosswalk_label(conn, label_org, origin_taxonomy_id=2, dst_taxonomy_id=1)
    assert label_dst == [('KW', 'PC')]
    assert is_equiv == False

    label_org = ("NRKW","PC")
    label_dst, is_equiv = klb.crosswalk_label(conn, label_org, origin_taxonomy_id=2, dst_taxonomy_id=3)
    assert label_dst == [('KW', 'PC')]
    assert is_equiv == False

    label_org = ("KW","PC")
    label_dst, is_equiv = klb.crosswalk_label(conn, label_org, origin_taxonomy_id=1, dst_taxonomy_id=4)
    assert label_dst == [('SRKW', 'PC')]
    assert is_equiv == True
