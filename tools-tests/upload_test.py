# Copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included
# in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
# in that file, in accordance with the Business Source License, use of this
# software will be governed by the Apache License, Version 2.0, included in
# the file licenses/APL2.txt.

import os
import pathlib
import ssl
import unittest.mock

import pytest
import sgcollect
import tasks
import trustme

ZIP_NAME = "foo.zip"
REDACTED_ZIP_NAME = "foo-redacted.zip"


@pytest.fixture
def main_norun(tmpdir):
    workdir = pathlib.Path.cwd()
    try:
        os.chdir(tmpdir)
        with unittest.mock.patch("tasks.TaskRunner.run"):
            with open(ZIP_NAME, "w"):
                pass
            yield
    finally:
        os.chdir(workdir)


@pytest.fixture
def main_norun_redacted_zip(main_norun):
    with open(REDACTED_ZIP_NAME, "w"):
        pass
    yield


class FakeResponse:
    def __init__(self, status_code):
        self.status_code = status_code

    def getcode(self):
        return self.status_code


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
    with pytest.raises(SystemExit, check=lambda e: e.code == 0):
        with unittest.mock.patch("sys.argv", ["sg_collect", *args, ZIP_NAME]):
            sgcollect.main()
    assert pathlib.Path(ZIP_NAME).exists()
    assert not pathlib.Path(REDACTED_ZIP_NAME).exists()


@pytest.mark.usefixtures("main_norun_redacted_zip")
def test_main_output_exists_with_redacted():
    with pytest.raises(SystemExit, check=lambda e: e.code == 0):
        with unittest.mock.patch(
            "sys.argv", ["sg_collect", "--log-redaction-level", "partial", ZIP_NAME]
        ):
            sgcollect.main()
    assert pathlib.Path(ZIP_NAME).exists()
    assert pathlib.Path(REDACTED_ZIP_NAME).exists()


@pytest.mark.usefixtures("main_norun")
@pytest.mark.parametrize("args", [[], ["--log-redaction-level", "none"]])
def test_main_zip_deleted_on_upload_success(args):
    with unittest.mock.patch("tasks.urllib.request.build_opener", FakeSuccessUrlOpener):
        with unittest.mock.patch(
            "sys.argv",
            [
                "sg_collect",
                *args,
                "--upload-host",
                "https://example.com",
                "--customer",
                "fakeCustomer",
                ZIP_NAME,
            ],
        ):
            with pytest.raises(SystemExit) as exc:
                sgcollect.main()
            assert exc.value.code == 0
    assert not pathlib.Path(ZIP_NAME).exists()
    assert not pathlib.Path(REDACTED_ZIP_NAME).exists()


@pytest.mark.usefixtures("main_norun")
@pytest.mark.parametrize("args", [[], ["--log-redaction-level", "none"]])
def test_main_zip_deleted_on_upload_failure(args):
    with unittest.mock.patch("tasks.urllib.request.build_opener", FakeFailureUrlOpener):
        with unittest.mock.patch(
            "sys.argv",
            [
                "sg_collect",
                *args,
                "--upload-host",
                "https://example.com",
                "--customer",
                "fakeCustomer",
                ZIP_NAME,
            ],
        ):
            with pytest.raises(SystemExit) as exc:
                sgcollect.main()
            assert exc.value.code == 1
    assert not pathlib.Path(ZIP_NAME).exists()
    assert not pathlib.Path(REDACTED_ZIP_NAME).exists()


@pytest.mark.usefixtures("main_norun_redacted_zip")
def test_main_redacted_zip_deleted_on_upload_success():
    with unittest.mock.patch("tasks.urllib.request.build_opener", FakeSuccessUrlOpener):
        with unittest.mock.patch(
            "sys.argv",
            [
                "sg_collect",
                "--log-redaction-level",
                "partial",
                "--upload-host",
                "https://example.com",
                "--customer",
                "fakeCustomer",
                ZIP_NAME,
            ],
        ):
            with pytest.raises(SystemExit) as exc:
                sgcollect.main()
            assert exc.value.code == 0
    assert not pathlib.Path(ZIP_NAME).exists()
    assert not pathlib.Path(REDACTED_ZIP_NAME).exists()


@pytest.mark.usefixtures("main_norun_redacted_zip")
def test_main_redacted_zip_deleted_on_upload_failure():
    with unittest.mock.patch("tasks.urllib.request.build_opener", FakeFailureUrlOpener):
        with unittest.mock.patch(
            "sys.argv",
            [
                "sg_collect",
                "--log-redaction-level",
                "partial",
                "--upload-host",
                "https://example.com",
                "--customer",
                "fakeCustomer",
                ZIP_NAME,
            ],
        ):
            with pytest.raises(SystemExit) as exc:
                sgcollect.main()
            assert exc.value.code == 1
    assert not pathlib.Path(ZIP_NAME).exists()
    assert not pathlib.Path(REDACTED_ZIP_NAME).exists()


@pytest.mark.usefixtures("main_norun")
@pytest.mark.parametrize("args", [[], ["--log-redaction-level", "none"]])
def test_main_keep_zip_on_upload_success(args):
    with unittest.mock.patch("tasks.urllib.request.build_opener", FakeSuccessUrlOpener):
        with unittest.mock.patch(
            "sys.argv",
            [
                "sg_collect",
                *args,
                "--upload-host",
                "https://example.com",
                "--customer",
                "fakeCustomer",
                "--keep-zip",
                ZIP_NAME,
            ],
        ):
            with pytest.raises(SystemExit) as exc:
                sgcollect.main()
            assert exc.value.code == 0
    assert pathlib.Path(ZIP_NAME).exists()
    assert not pathlib.Path(REDACTED_ZIP_NAME).exists()


@pytest.mark.usefixtures("main_norun")
@pytest.mark.parametrize("args", [[], ["--log-redaction-level", "none"]])
def test_main_keep_zip_on_upload_failure(args):
    with unittest.mock.patch("tasks.urllib.request.build_opener", FakeFailureUrlOpener):
        with unittest.mock.patch(
            "sys.argv",
            [
                "sg_collect",
                *args,
                "--upload-host",
                "https://example.com",
                "--customer",
                "fakeCustomer",
                "--keep-zip",
                ZIP_NAME,
            ],
        ):
            with pytest.raises(SystemExit) as exc:
                sgcollect.main()
            assert exc.value.code == 1
    assert pathlib.Path(ZIP_NAME).exists()
    assert not pathlib.Path(REDACTED_ZIP_NAME).exists()


@pytest.mark.usefixtures("main_norun_redacted_zip")
def test_main_keep_zip_deleted_on_upload_success():
    with unittest.mock.patch("tasks.urllib.request.build_opener", FakeSuccessUrlOpener):
        with unittest.mock.patch(
            "sys.argv",
            [
                "sg_collect",
                "--log-redaction-level",
                "partial",
                "--upload-host",
                "https://example.com",
                "--customer",
                "fakeCustomer",
                "--keep-zip",
                ZIP_NAME,
            ],
        ):
            with pytest.raises(SystemExit) as exc:
                sgcollect.main()
            assert exc.value.code == 0
    assert pathlib.Path(ZIP_NAME).exists()
    assert pathlib.Path(REDACTED_ZIP_NAME).exists()


@pytest.mark.usefixtures("main_norun_redacted_zip")
def test_main_keep_zip_deleted_on_upload_failure():
    with unittest.mock.patch("tasks.urllib.request.build_opener", FakeFailureUrlOpener):
        with unittest.mock.patch(
            "sys.argv",
            [
                "sg_collect",
                "--log-redaction-level",
                "partial",
                "--upload-host",
                "https://example.com",
                "--customer",
                "fakeCustomer",
                "--keep-zip",
                ZIP_NAME,
            ],
        ):
            with pytest.raises(SystemExit) as exc:
                sgcollect.main()
            assert exc.value.code == 1
    assert pathlib.Path(ZIP_NAME).exists()
    assert pathlib.Path(REDACTED_ZIP_NAME).exists()


@pytest.fixture(scope="session")
def httpserver_ssl_context():
    ca = trustme.CA()
    client_context = ssl.SSLContext()
    server_context = ssl.SSLContext()
    server_cert = ca.issue_cert("test-host.example.org")
    ca.configure_trust(client_context)
    server_cert.configure_cert(server_context)

    def default_context():
        return client_context

    ssl._create_default_https_context = default_context

    return server_context


def test_stream_large_file(tmpdir, httpserver):
    """
    Write a file greater than 2GB to make sure it does not throw an exception.
    """
    p = tmpdir.join("testfile.txt")
    with open(p, "wb") as f:
        for i in range(2200):
            f.write(os.urandom(1_000_000))

    def handler(request):
        pass

    httpserver.expect_request("/").respond_with_handler(handler)
    assert tasks.do_upload(p, httpserver.url_for("/"), "") == 0

    httpserver.check()


def test_stream_file(tmpdir, httpserver):
    """
    Make sure that streaming the contents of a file show up when streaming.
    """
    p = tmpdir.join("testfile.txt")
    body = "foobar"
    p.write(body)
    r = None

    def handler(request):
        nonlocal r
        r = request

    httpserver.expect_request("/").respond_with_handler(handler)
    assert tasks.do_upload(p, httpserver.url_for("/"), "") == 0

    httpserver.check()

    assert r.headers.get("Content-Length") == "6"
    assert r.headers.get("Transfer-Encoding") is None
    assert r.data == body.encode()
