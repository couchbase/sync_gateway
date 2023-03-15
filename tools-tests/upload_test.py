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
REDACTED_ZIP_NAME = "foo-redacted.zip"

@pytest.fixture
def main_norun(tmpdir):
    workdir = pathlib.Path.cwd()
    try:
        os.chdir(tmpdir)
        with unittest.mock.patch("tasks.TaskRunner.run"):
            with open(ZIP_NAME, "w"):
                    yield
    finally:
        os.chdir(workdir)

class FakeResponse:
    def __init__(self, status_code):
        self.status_code = status_code

    def getcode(self):
        return self.status_code

class FakeFailureUrlOpener:

    def __init__(self, *args, **kwargs):
        pass

    def open(request):
        return FakeResponse(500)

class FakeSuccessUrlOpener:

    def __init__(self, *args, **kwargs):
        pass

    def open(self, *args, **kwargs):
        return FakeResponse(200)

class FakeFailureUrlOpener:

    def __init__(self, *args, **kwargs):
        pass

    def open(self, *args, **kwargs):
        return FakeResponse(500)


@pytest.mark.usefixtures("main_norun")
@pytest.mark.parametrize("args", [[], ["--log-redaction-level", "none"]])
def test_main_output_exists(args):
    with unittest.mock.patch("sys.argv", ["sg_collect", *args, ZIP_NAME]):
            sgcollect_info.main()
    assert pathlib.Path(ZIP_NAME).exists()
    assert not pathlib.Path(REDACTED_ZIP_NAME).exists()

@pytest.mark.usefixtures("main_norun")
def test_main_output_exists_with_redacted():
    with unittest.mock.patch("sys.argv", ["sg_collect", "--log-redaction-level", "partial", ZIP_NAME]):
            sgcollect_info.main()
    assert pathlib.Path(ZIP_NAME).exists()
    assert pathlib.Path(REDACTED_ZIP_NAME).exists()

@pytest.mark.usefixtures("main_norun")
@pytest.mark.parametrize("args", [[], ["--log-redaction-level", "none"]])
def test_main_zip_deleted_on_upload_success(args):
    with unittest.mock.patch("tasks.urllib.request.build_opener", FakeSuccessUrlOpener):
        with unittest.mock.patch("sys.argv", ["sg_collect", *args, "--upload-host", "https://nohost.com", "--customer", "fakeCustomer",  ZIP_NAME]):
            with pytest.raises(SystemExit) as exc:
                sgcollect_info.main()
            assert exc.value.code == 0
    assert not pathlib.Path(ZIP_NAME).exists()
    assert not pathlib.Path(REDACTED_ZIP_NAME).exists()

@pytest.mark.usefixtures("main_norun")
@pytest.mark.parametrize("args", [[], ["--log-redaction-level", "none"]])
def test_main_zip_deleted_on_upload_failure(args):
    with unittest.mock.patch("tasks.urllib.request.build_opener", FakeFailureUrlOpener):
        with unittest.mock.patch("sys.argv", ["sg_collect", *args, "--upload-host", "https://nohost.com", "--customer", "fakeCustomer",  ZIP_NAME]):
            with pytest.raises(SystemExit) as exc:
                sgcollect_info.main()
            assert exc.value.code == 1
    assert not pathlib.Path(ZIP_NAME).exists()
    assert not pathlib.Path(REDACTED_ZIP_NAME).exists()
