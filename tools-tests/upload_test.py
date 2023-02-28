# Copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included
# in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
# in that file, in accordance with the Business Source License, use of this
# software will be governed by the Apache License, Version 2.0, included in
# the file licenses/APL2.txt.

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
