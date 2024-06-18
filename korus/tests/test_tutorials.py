import os
import pytest
import subprocess

current_dir = os.path.dirname(os.path.realpath(__file__))
path_to_assets = os.path.join(current_dir, "assets")
path_to_tmp = os.path.join(path_to_assets, "tmp")


def test_tutorials():
    """ Check that all Jupyter Notebook tutorials complete successfully 
    and generate the expected output files (we only check that the files 
    exist, not that they have the expected content)"""
    bash_script_path = os.path.join(current_dir, "run_tutorials.sh")
    res = subprocess.check_output([bash_script_path]).decode()
    assert res == "tax_t1.sqlite created\nmydb.sqlite created\n"
