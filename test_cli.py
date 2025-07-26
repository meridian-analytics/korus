import os
import pytest
import korus.cli.cli as cli

current_dir = os.path.dirname(os.path.realpath(__file__))
path_to_assets = os.path.join(current_dir, "assets")
path_to_tmp = os.path.join(path_to_assets, "tmp")


def test_test():
    pass


cli.cli_fcn("abc")

