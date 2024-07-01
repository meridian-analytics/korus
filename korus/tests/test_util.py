import os
import pytest
import korus.util as ku


# TODO: add tests for all functions in korus.util module


current_dir = os.path.dirname(os.path.realpath(__file__))
path_to_assets = os.path.join(current_dir, "assets")
path_to_tmp = os.path.join(path_to_assets, "tmp")


def test_find_files_dir():
    """Check that we can find files in a directory"""
    path = os.path.join(path_to_assets, "files")
    f = ku.find_files(path)
    f.sort()
    assert f == ["a.txt", "b.wav"]
    f = ku.find_files(path, subdirs=True)
    f.sort()
    assert f == ["a.txt", "b.wav", "more-files/c.flac", "more-files/d.wav", "more-files/e.txt"]
    f = ku.find_files(path, substr="wav", subdirs=True)
    f.sort()
    assert f == ["b.wav", "more-files/d.wav"]

def test_find_files_tar():
    """Check that we can find files in a zipped tar archive"""
    path = os.path.join(path_to_assets, "zipped-files.tar.gz")
    f = ku.find_files(path)
    f.sort()
    assert f == ["a.txt", "b.wav", "more-files/c.flac", "more-files/d.wav", "more-files/e.txt"]
    f = ku.find_files(path, substr="wav")
    f.sort()
    assert f == ["b.wav", "more-files/d.wav"]
