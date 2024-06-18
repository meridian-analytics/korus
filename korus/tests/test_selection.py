import os
import numpy as np
import pytest
import sqlite3
import korus.db as kdb
import korus.selection as ks

current_dir = os.path.dirname(os.path.realpath(__file__))
path_to_assets = os.path.join(current_dir, "assets")
path_to_tmp = os.path.join(path_to_assets, "tmp")

np.random.seed(1)


def test_create_selections(db_with_annotations):
    conn, db_path = db_with_annotations
    idx_kw = kdb.filter_annotation(conn, source_type=("KW","PC"), taxonomy_id=3)
    assert len(idx_kw) == 2

    # create KW selections without stepping
    df = ks.create_selections(conn, indices=idx_kw, window_ms=3000,)
    assert len(df) == 2
    assert np.all(df.columns.values == ["start", "end", "annot_id"])
    assert np.all(df.index.names == ["sel_id", "filename"])
    assert np.all(np.isclose((df.end - df.start).values, 3.0, rtol=1e-3))

    # create KW selections with stepping
    df = ks.create_selections(conn, indices=idx_kw, window_ms=3000, step_ms=1000, full_path=False)

    # two selections created from first annotation (1.3s duration)
    df_1 = df[df.annot_id == 1]
    assert len(df_1) == 2

    # about 300 selections created from second annotation (300s duration)
    df_2 = df[df.annot_id == 2]
    assert len(df_2) == 300

    # create KW selections with stepping and cap on max number of selections
    df = ks.create_selections(conn, indices=idx_kw, window_ms=3000, step_ms=1000, num_max=10)
    df_1 = df[df.annot_id == 0]
    assert len(df_1) == 0
    df_2 = df[df.annot_id == 2]
    assert len(df_2) == 10

    # create KW selections with exclusive=True and window=1min and step=50s
    df = ks.create_selections(conn, indices=idx_kw, window_ms=60000, step_ms=50000, exclusive=True)
    df_1 = df[df.annot_id == 0]
    assert len(df_1) == 0
    df_2 = df[df.annot_id == 2]
    assert len(df_2.index.get_level_values(0).unique()) == 5
    assert df.iloc[0].start > 21.2
    assert df.iloc[-1].end < 21.2
    

