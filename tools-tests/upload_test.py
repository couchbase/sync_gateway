# Copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included
# in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
# in that file, in accordance with the Business Source License, use of this
# software will be governed by the Apache License, Version 2.0, included in
# the file licenses/APL2.txt.

import http.client
import io
import math
import os
import pathlib
import ssl
import unittest.mock
import urllib.error
import urllib.response
import zipfile
from typing import Any

import pytest
import sgcollect
import tasks
import trustme
from pytest_httpserver import HTTPServer
from werkzeug import Request, Response

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


@pytest.fixture
def taskrunner_workdir(tmp_path: pathlib.Path):
    """
    Creates a temporary workdir for the TaskRunner to use. The directory is required to exist before TaskRunner is
    instantiated and closed when TaskRunner goes out of scope.
    """
    workdir = tmp_path / "workdir"
    workdir.mkdir()
    yield workdir


class FakeSuccessUrlOpener:
    def __init__(self, *args, **kwargs):
        pass

    def open(self, request, *args, **kwargs):
        return urllib.response.addinfourl(
            io.BytesIO(b"{}"), http.client.HTTPMessage(), request.full_url, 200
        )


class FakeFailureUrlOpener:
    def __init__(self, *args, **kwargs):
        pass

    def open(self, request, *args, **kwargs):
        raise urllib.error.HTTPError(
            request.full_url, 500, "error", http.client.HTTPMessage(), io.BytesIO(b"{}")
        )


@pytest.mark.usefixtures("main_norun")
@pytest.mark.parametrize("args", [[], ["--log-redaction-level", "none"]])
def test_main_output_exists(args, taskrunner_workdir):
    with (
        pytest.raises(SystemExit, check=lambda e: e.code == 0),
        unittest.mock.patch(
            "sys.argv", ["sg_collect", *args, "--tmp-dir", taskrunner_workdir, ZIP_NAME]
        ),
    ):
        sgcollect.main()
    assert pathlib.Path(ZIP_NAME).exists()
    assert not pathlib.Path(REDACTED_ZIP_NAME).exists()
    assert not [x for x in taskrunner_workdir.iterdir()]


@pytest.mark.usefixtures("main_norun_redacted_zip")
def test_main_output_exists_with_redacted(taskrunner_workdir):
    with (
        pytest.raises(SystemExit, check=lambda e: e.code == 0),
        unittest.mock.patch(
            "sys.argv",
            [
                "sg_collect",
                "--log-redaction-level",
                "partial",
                "--tmp-dir",
                taskrunner_workdir,
                ZIP_NAME,
            ],
        ),
    ):
        sgcollect.main()
    assert pathlib.Path(ZIP_NAME).exists()
    assert pathlib.Path(REDACTED_ZIP_NAME).exists()
    assert not [x for x in taskrunner_workdir.iterdir()]


@pytest.mark.usefixtures("main_norun")
@pytest.mark.parametrize("args", [[], ["--log-redaction-level", "none"]])
def test_main_zip_deleted_on_upload_success(args, taskrunner_workdir):
    with (
        unittest.mock.patch("tasks.urllib.request.build_opener", FakeSuccessUrlOpener),
        unittest.mock.patch(
            "sys.argv",
            [
                "sg_collect",
                *args,
                "--upload-host",
                "https://example.com",
                "--customer",
                "fakeCustomer",
                "--tmp-dir",
                taskrunner_workdir,
                ZIP_NAME,
            ],
        ),
    ):
        with pytest.raises(SystemExit) as exc:
            sgcollect.main()
        assert exc.value.code == 0
    assert not pathlib.Path(ZIP_NAME).exists()
    assert not pathlib.Path(REDACTED_ZIP_NAME).exists()
    assert not [x for x in taskrunner_workdir.iterdir()]


@pytest.mark.usefixtures("main_norun")
@pytest.mark.parametrize("args", [[], ["--log-redaction-level", "none"]])
def test_main_zip_deleted_on_upload_failure(args, taskrunner_workdir):
    with (
        unittest.mock.patch("tasks.urllib.request.build_opener", FakeFailureUrlOpener),
        unittest.mock.patch(
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
        ),
    ):
        with pytest.raises(SystemExit) as exc:
            sgcollect.main()
        assert exc.value.code == 1
    assert not pathlib.Path(ZIP_NAME).exists()
    assert not pathlib.Path(REDACTED_ZIP_NAME).exists()
    assert not [x for x in taskrunner_workdir.iterdir()]


@pytest.mark.usefixtures("main_norun_redacted_zip")
def test_main_redacted_zip_deleted_on_upload_success(taskrunner_workdir):
    with (
        unittest.mock.patch("tasks.urllib.request.build_opener", FakeSuccessUrlOpener),
        unittest.mock.patch(
            "sys.argv",
            [
                "sg_collect",
                "--log-redaction-level",
                "partial",
                "--upload-host",
                "https://example.com",
                "--customer",
                "fakeCustomer",
                "--tmp-dir",
                taskrunner_workdir,
                ZIP_NAME,
            ],
        ),
    ):
        with pytest.raises(SystemExit) as exc:
            sgcollect.main()
        assert exc.value.code == 0
    assert not pathlib.Path(ZIP_NAME).exists()
    assert not pathlib.Path(REDACTED_ZIP_NAME).exists()
    assert not [x for x in taskrunner_workdir.iterdir()]


@pytest.mark.usefixtures("main_norun_redacted_zip")
def test_main_redacted_zip_deleted_on_upload_failure(taskrunner_workdir):
    with (
        unittest.mock.patch("tasks.urllib.request.build_opener", FakeFailureUrlOpener),
        unittest.mock.patch(
            "sys.argv",
            [
                "sg_collect",
                "--log-redaction-level",
                "partial",
                "--upload-host",
                "https://example.com",
                "--customer",
                "fakeCustomer",
                "--tmp-dir",
                taskrunner_workdir,
                ZIP_NAME,
            ],
        ),
    ):
        with pytest.raises(SystemExit) as exc:
            sgcollect.main()
        assert exc.value.code == 1
    assert not pathlib.Path(ZIP_NAME).exists()
    assert not pathlib.Path(REDACTED_ZIP_NAME).exists()
    assert not [x for x in taskrunner_workdir.iterdir()]


@pytest.mark.usefixtures("main_norun")
@pytest.mark.parametrize("args", [[], ["--log-redaction-level", "none"]])
def test_main_keep_zip_on_upload_success(args, taskrunner_workdir):
    with (
        unittest.mock.patch("tasks.urllib.request.build_opener", FakeSuccessUrlOpener),
        unittest.mock.patch(
            "sys.argv",
            [
                "sg_collect",
                *args,
                "--upload-host",
                "https://example.com",
                "--customer",
                "fakeCustomer",
                "--keep-zip",
                "--tmp-dir",
                taskrunner_workdir,
                ZIP_NAME,
            ],
        ),
    ):
        with pytest.raises(SystemExit) as exc:
            sgcollect.main()
        assert exc.value.code == 0
    assert pathlib.Path(ZIP_NAME).exists()
    assert not pathlib.Path(REDACTED_ZIP_NAME).exists()
    assert not [x for x in taskrunner_workdir.iterdir()]


@pytest.mark.usefixtures("main_norun")
@pytest.mark.parametrize("args", [[], ["--log-redaction-level", "none"]])
def test_main_keep_zip_on_upload_failure(args, taskrunner_workdir):
    with (
        unittest.mock.patch("tasks.urllib.request.build_opener", FakeFailureUrlOpener),
        unittest.mock.patch(
            "sys.argv",
            [
                "sg_collect",
                *args,
                "--upload-host",
                "https://example.com",
                "--customer",
                "fakeCustomer",
                "--keep-zip",
                "--tmp-dir",
                taskrunner_workdir,
                ZIP_NAME,
            ],
        ),
    ):
        with pytest.raises(SystemExit) as exc:
            sgcollect.main()
        assert exc.value.code == 1
    assert pathlib.Path(ZIP_NAME).exists()
    assert not pathlib.Path(REDACTED_ZIP_NAME).exists()
    assert not [x for x in taskrunner_workdir.iterdir()]


@pytest.mark.usefixtures("main_norun_redacted_zip")
def test_main_keep_zip_deleted_on_upload_success(taskrunner_workdir):
    with (
        unittest.mock.patch("tasks.urllib.request.build_opener", FakeSuccessUrlOpener),
        unittest.mock.patch(
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
                "--tmp-dir",
                taskrunner_workdir,
                ZIP_NAME,
            ],
        ),
    ):
        with pytest.raises(SystemExit) as exc:
            sgcollect.main()
        assert exc.value.code == 0
    assert pathlib.Path(ZIP_NAME).exists()
    assert pathlib.Path(REDACTED_ZIP_NAME).exists()
    assert not [x for x in taskrunner_workdir.iterdir()]


@pytest.mark.usefixtures("main_norun_redacted_zip")
def test_main_keep_zip_deleted_on_upload_failure(taskrunner_workdir):
    with (
        unittest.mock.patch("tasks.urllib.request.build_opener", FakeFailureUrlOpener),
        unittest.mock.patch(
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
                "--tmp-dir",
                taskrunner_workdir,
                ZIP_NAME,
            ],
        ),
    ):
        with pytest.raises(SystemExit) as exc:
            sgcollect.main()
        assert exc.value.code == 1
    assert pathlib.Path(ZIP_NAME).exists()
    assert pathlib.Path(REDACTED_ZIP_NAME).exists()
    assert not [x for x in taskrunner_workdir.iterdir()]


@pytest.fixture(scope="session")
def httpserver_ssl_context():
    """
    pytest-httpserver serves HTTPS when this fixture exists. The certificate is untrusted, so the tests cover the
    unverified TLS context in do_upload.
    """
    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    trustme.CA().issue_cert("localhost").configure_cert(server_context)
    return server_context


def test_stream_large_file(tmpdir, httpserver):
    """
    Write a file greater than 2GB to make sure it does not throw an exception.
    """
    p = tmpdir.join("testfile.txt")
    with open(p, "wb") as f:
        f.writelines(os.urandom(1_000_000) for i in range(2200))

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
    r: Any = None

    def handler(request):
        nonlocal r
        r = request

    httpserver.expect_request("/").respond_with_handler(handler)
    assert tasks.do_upload(p, httpserver.url_for("/"), "") == 0

    httpserver.check()

    assert r.headers.get("Content-Length") == "6"
    assert r.headers.get("Transfer-Encoding") is None
    assert r.data == body.encode()


def record_uploads(httpserver: HTTPServer, paths: list[str]) -> dict[str, bytes]:
    """
    Expect a PUT to each path and return a dict that fills with path -> body in upload order.
    """
    uploads: dict[str, bytes] = {}

    def handler(request: Request) -> Response:
        assert request.headers.get("Content-Length") == str(len(request.data))
        uploads[request.path] = request.data
        return Response("")

    for path in paths:
        httpserver.expect_request(path, method="PUT").respond_with_handler(handler)
    return uploads


def test_split_upload_reassembles(
    tmp_path: pathlib.Path, httpserver: HTTPServer
) -> None:
    """
    Support joins the parts with `cat foo.zip.*`, so make sure that joining the parts in name order gives back a valid
    zip.
    """
    part_size = 1024
    zip_path = tmp_path / ZIP_NAME
    members = {f"file{i}.txt": os.urandom(1000) for i in range(5)}
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_STORED) as zf:
        for name, data in members.items():
            zf.writestr(name, data)
    num_parts = math.ceil(zip_path.stat().st_size / part_size)
    assert num_parts > 3

    paths = [f"/{ZIP_NAME}.{i:03d}" for i in range(num_parts)]
    uploads = record_uploads(httpserver, paths)
    with unittest.mock.patch("tasks.MAX_UPLOAD_PART_SIZE", part_size):
        assert tasks.do_upload(zip_path, httpserver.url_for(f"/{ZIP_NAME}"), "") == 0
    httpserver.check()

    assert list(uploads) == [paths[i] for i in tasks.upload_part_order(num_parts)]
    assert all(len(uploads[p]) == part_size for p in paths[:-1])
    assert 0 < len(uploads[paths[-1]]) <= part_size

    reassembled = tmp_path / "reassembled.zip"
    reassembled.write_bytes(b"".join(uploads[p] for p in paths))
    assert reassembled.read_bytes() == zip_path.read_bytes()
    with zipfile.ZipFile(reassembled) as zf:
        assert zf.testzip() is None
        assert {name: zf.read(name) for name in zf.namelist()} == members


@pytest.mark.parametrize(
    "size, expected_suffixes",
    [
        pytest.param(10, [""], id="under limit"),
        pytest.param(100, [""], id="exactly limit"),
        pytest.param(101, [".001", ".000"], id="limit plus one"),
        pytest.param(300, [".002", ".000", ".001"], id="exact multiple"),
    ],
)
def test_split_upload_sizes(
    tmp_path: pathlib.Path,
    httpserver: HTTPServer,
    size: int,
    expected_suffixes: list[str],
) -> None:
    """
    A file at or below the limit uploads in one request with no suffix. One byte over the limit adds a part. The last
    part uploads first.
    """
    p = tmp_path / ZIP_NAME
    body = os.urandom(size)
    p.write_bytes(body)

    paths = [f"/{ZIP_NAME}{suffix}" for suffix in expected_suffixes]
    uploads = record_uploads(httpserver, paths)
    with unittest.mock.patch("tasks.MAX_UPLOAD_PART_SIZE", 100):
        assert tasks.do_upload(p, httpserver.url_for(f"/{ZIP_NAME}"), "") == 0
    httpserver.check()

    assert list(uploads) == paths
    assert b"".join(uploads[p] for p in sorted(paths)) == body


def test_split_upload_part_failure(
    tmp_path: pathlib.Path,
    httpserver: HTTPServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """
    Make sure that a failed part stops the upload and reports the URL, status code and body without a traceback.
    """
    p = tmp_path / ZIP_NAME
    p.write_bytes(os.urandom(450))

    httpserver.expect_ordered_request(f"/{ZIP_NAME}.004").respond_with_data("")
    httpserver.expect_ordered_request(f"/{ZIP_NAME}.000").respond_with_data(
        "bad part", status=500
    )
    with unittest.mock.patch("tasks.MAX_UPLOAD_PART_SIZE", 100):
        assert tasks.do_upload(p, httpserver.url_for(f"/{ZIP_NAME}"), "") == 1
    httpserver.check()
    assert len(httpserver.log) == 2
    err = capsys.readouterr().err
    assert httpserver.url_for(f"/{ZIP_NAME}.000") in err
    assert "status code: 500, body: bad part" in err
    assert "Traceback" not in err


def test_split_upload_url_with_query(
    tmp_path: pathlib.Path, httpserver: HTTPServer
) -> None:
    """
    Make sure that part suffixes go on the URL path and the query string is kept for every part.
    """
    p = tmp_path / ZIP_NAME
    p.write_bytes(os.urandom(150))

    for i in range(2):
        httpserver.expect_request(
            f"/{ZIP_NAME}.{i:03d}", method="PUT", query_string="sig=abc"
        ).respond_with_data("")
    url = httpserver.url_for(f"/{ZIP_NAME}") + "?sig=abc"
    with unittest.mock.patch("tasks.MAX_UPLOAD_PART_SIZE", 100):
        assert tasks.do_upload(p, url, "") == 0
    httpserver.check()


@pytest.mark.parametrize(
    "opener, exit_code",
    [
        pytest.param(FakeSuccessUrlOpener, 0, id="success"),
        pytest.param(FakeFailureUrlOpener, 1, id="failure"),
    ],
)
def test_main_just_upload_into_exits(
    tmp_path: pathlib.Path, opener: type, exit_code: int
) -> None:
    """
    Make sure that --just-upload-into exits with the upload result and does not run a collection.
    """
    p = tmp_path / ZIP_NAME
    p.write_bytes(b"data")
    with (
        unittest.mock.patch("tasks.urllib.request.build_opener", opener),
        unittest.mock.patch("sgcollect.TaskRunner") as runner,
        unittest.mock.patch(
            "sys.argv",
            ["sg_collect", "--just-upload-into", "https://example.com/foo.zip", str(p)],
        ),
        pytest.raises(SystemExit, check=lambda e: e.code == exit_code),
    ):
        sgcollect.main()
    runner.assert_not_called()
