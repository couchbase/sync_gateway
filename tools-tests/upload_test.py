import os
import pathlib
import unittest.mock
import urllib.error

import pytest

import sgcollect_info
import tasks

ZIP_NAME = "foo.zip"

@pytest.fixture
def main_norun():
    with unittest.mock.patch("tasks.TaskRunner.run"):
        with unittest.mock.patch("sys.argv", ["sg_collect", ZIP_NAME]):
            yield
    os.remove(ZIP_NAME)

@pytest.mark.usefixtures('main_norun')
def test_main():
    sgcollect_info.main()
    assert pathlib.Path(ZIP_NAME).exists()
