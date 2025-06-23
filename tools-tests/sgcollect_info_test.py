# Copyright 2024-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included
# in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
# in that file, in accordance with the Business Source License, use of this
# software will be governed by the Apache License, Version 2.0, included in
# the file licenses/APL2.txt.

import io
import os
import pathlib
import unittest.mock
import urllib.error
from typing import Optional

import pytest
import sgcollect
import tasks


@pytest.mark.parametrize(
    "config",
    [
        '{{"logfilepath": "{tmpdir}"}}',
        '{{"Logging": {{ "default": {{ "logfilepath": "{log_file}" }} }} }}',
        '{{"logging": {{ "log_file_path": "{tmpdir}" }} }}',
    ],
)
def test_make_collect_logs_tasks(config, tmpdir):
    log_file = tmpdir.join("sg_info.log")
    log_file.write("foo")
    with unittest.mock.patch(
        "sgcollect.urlopen_with_basic_auth",
        return_value=io.BytesIO(
            config.format(
                tmpdir=str(tmpdir).replace("\\", "\\\\"),
                log_file=str(log_file).replace("\\", "\\\\"),
            ).encode("utf-8")
        ),
    ):
        rotated_log_file = tmpdir.join("sg_info-01.log.gz")
        rotated_log_file.write("foo")
        tasks = sgcollect.make_collect_logs_tasks(
            sg_url="fakeurl",
            sg_config_file_path="",
            sg_username="",
            sg_password="",
        )
        assert [t.log_file for t in tasks] == [
            log_file.basename,
            rotated_log_file.basename,
        ]


def test_make_collect_logs_heap_profile(tmpdir):
    with unittest.mock.patch(
        "sgcollect.urlopen_with_basic_auth",
        return_value=io.BytesIO(
            '{{"logfilepath": "{logpath}"}}'.format(
                logpath=str(tmpdir).replace("\\", "\\\\")
            ).encode("utf-8")
        ),
    ):
        pprof_file = tmpdir.join("pprof_heap_high_01.pb.gz")
        pprof_file.write("foo")
        tasks = sgcollect.make_collect_logs_tasks(
            sg_url="fakeurl",
            sg_config_file_path="",
            sg_username="",
            sg_password="",
        )
        assert [tasks[0].log_file] == [pprof_file.basename]
        # ensure that this is not redacted task
        assert tasks[0].description.startswith("Contents of")


@pytest.mark.parametrize("should_redact", [True, False])
def test_make_collect_logs_tasks_duplicate_files(should_redact, tmp_path):
    tmpdir1 = tmp_path / "tmpdir1"
    tmpdir2 = tmp_path / "tmpdir2"
    config = """
        {{"logfilepath": "{tmpdir1}",
          "logging": {{ "log_file_path": "{tmpdir2}" }}
        }}
    """
    for d in [tmpdir1, tmpdir2]:
        d.mkdir()
        (d / "sg_info.log").write_text("foo")
        (d / "sg_info-01.log.gz").write_text("foo")

    with unittest.mock.patch(
        "sgcollect.urlopen_with_basic_auth",
        return_value=io.BytesIO(
            config.format(
                tmpdir1=str(tmpdir1).replace("\\", "\\\\"),
                tmpdir2=str(tmpdir2).replace("\\", "\\\\"),
            ).encode("utf-8")
        ),
    ):
        tasks = sgcollect.make_collect_logs_tasks(
            sg_url="fakeurl",
            sg_config_file_path="",
            sg_username="",
            sg_password="",
        )
        # assert all tasks have unique log_file names
        assert len(set(t.log_file for t in tasks)) == len(tasks)
        assert set(t.log_file for t in tasks) == {
            "sg_info.log",
            "sg_info.log.1",
            "sg_info-01.log.gz",
            "sg_info-01.log.gz.1",
        }


@pytest.mark.parametrize(
    "basenames, filename, expected",
    [
        ({}, "foo", "foo"),
        ({"foo"}, "foo", "foo.1"),
        ({"foo", "foo.1"}, "foo", "foo.2"),
    ],
)
def test_get_unique_filename(basenames, filename, expected):
    assert sgcollect.get_unique_filename(basenames, filename) == expected


@pytest.mark.parametrize(
    "cmdline",
    [
        ([]),
        (["--log-redaction-level", "full"]),
        (["--log-redaction-level", "none"]),
        (["--log-redaction-l", "none"]),
        (["--sync-gateway-password=mypassword"]),
        (["--sync-gateway-pa=mypassword"]),
        (["--sync-gateway-password", "mypassword"]),
    ],
)
def test_get_sgcollect_options_task(tmp_path, cmdline):
    runner = tasks.TaskRunner(tmp_dir=tmp_path)
    parser = sgcollect.create_option_parser()
    options, args = parser.parse_args(cmdline + ["fakesgcollect.zip"])
    task = sgcollect.get_sgcollect_info_options_task(options, args)
    runner.run(task)

    output = (
        pathlib.Path(runner.tmpdir) / sgcollect.SGCOLLECT_INFO_OPTIONS_LOG
    ).read_text()
    assert "sync_gateway_password" not in output
    assert f"args: {args}" in output


def test_get_paths_from_expvars_no_url() -> None:
    assert (None, None) == sgcollect.get_paths_from_expvars(
        sg_url="", sg_username="", sg_password=""
    )


@pytest.mark.parametrize(
    "expvar_output,expected_sg_path,expected_config_path",
    [
        (b"", None, None),
        (b"{}", None, None),
        (b'{"cmdline": []}', None, None),
        (b'{"cmdline": ["filename"]}', "filename", None),
        (
            b'{"cmdline": ["fake_sync_gateway", "real_file.txt"]}',
            "fake_sync_gateway",
            None,
        ),
        (
            b'{"cmdline": ["fake_sync_gateway", "-json", "fake_sync_gateway_config.json"]}',
            "fake_sync_gateway",
            "{cwd}{pathsep}fake_sync_gateway_config.json",
        ),
        (
            b'{"cmdline": ["fake_sync_gateway", "-json", "{tmpdir}/real_file.json"]}',
            "fake_sync_gateway",
            "{tmpdir}{pathsep}real_file.json",
        ),
    ],
)
def test_get_paths_from_expvars(
    expvar_output: bytes,
    expected_sg_path: Optional[str],
    expected_config_path: Optional[str],
    tmpdir: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_file = tmpdir / "real_file.json"
    real_file.write_text("This is a real file.", encoding="utf-8")
    subdir = tmpdir / "subdir"
    subdir.mkdir()
    monkeypatch.chdir(subdir)
    cwd = pathlib.Path.cwd()
    expvar_output = expvar_output.replace(b"{cwd}", str(cwd).encode("utf-8"))
    expvar_output = expvar_output.replace(b"{tmpdir}", str(tmpdir).encode("utf-8"))

    # interpolate cwd for pathlib.Path.resolve
    if expected_config_path is not None:
        expected_config_path = expected_config_path.format(
            cwd=str(cwd),
            tmpdir=str(tmpdir),
            pathsep=os.sep,
        )
    with unittest.mock.patch(
        "sgcollect.urlopen_with_basic_auth", return_value=io.BytesIO(expvar_output)
    ):
        sg_path, config_path = sgcollect.get_paths_from_expvars(
            sg_url="fakeurl", sg_username="", sg_password=""
        )
    assert sg_path == expected_sg_path
    assert config_path == expected_config_path


def test_discover_sg_binary_path() -> None:
    parser = sgcollect.create_option_parser()
    options, _ = parser.parse_args([])
    with unittest.mock.patch("os.path.exists", return_value=False):
        assert (
            sgcollect.discover_sg_binary_path(
                options,
                sg_url="",
            )
            == ""
        )
        options, _ = parser.parse_args(["--sync-gateway-executable", "fake_sg"])
        with pytest.raises(
            expected_exception=Exception,
            match="executable passed in does not exist",
        ):
            sgcollect.discover_sg_binary_path(options, sg_url="")

    options, _ = parser.parse_args([])
    with unittest.mock.patch("os.path.exists", return_value=True):
        assert (
            sgcollect.discover_sg_binary_path(options, sg_url="")
            == "/opt/couchbase-sync-gateway/bin/sync_gateway"
        )
    options, _ = parser.parse_args([])
    with unittest.mock.patch("os.path.exists", side_effect=[False, True]):
        assert (
            sgcollect.discover_sg_binary_path(options, sg_url="")
            == R"C:\Program Files (x86)\Couchbase\sync_gateway.exe"
        )  # Windows (Pre-2.0)

    with unittest.mock.patch("os.path.exists", side_effect=[False, False, True]):
        assert (
            sgcollect.discover_sg_binary_path(options, sg_url="")
            == R"C:\Program Files\Couchbase\Sync Gateway\sync_gateway.exe"  # Windows (Post-2.0)
        )


@pytest.mark.parametrize(
    "cmdline_args,expected_calls",
    [
        (
            [],
            [
                unittest.mock.call(
                    url="http://127.0.0.1:4985",
                    username=None,
                    password=None,
                ),
            ],
        ),
        (
            ["--sync-gateway-username=myuser", "--sync-gateway-password=mypassword"],
            [
                unittest.mock.call(
                    url="http://127.0.0.1:4985",
                    username="myuser",
                    password="mypassword",
                ),
            ],
        ),
        (
            ["--sync-gateway-url=example.com"],
            [
                unittest.mock.call(
                    url="http://example.com",
                    username=None,
                    password=None,
                ),
                unittest.mock.call(
                    url="https://example.com",
                    username=None,
                    password=None,
                ),
                unittest.mock.call(
                    url="http://127.0.0.1:4985",
                    username=None,
                    password=None,
                ),
            ],
        ),
        (
            ["--sync-gateway-url=https://example.com:4985"],
            [
                unittest.mock.call(
                    url="https://example.com:4985",
                    username=None,
                    password=None,
                ),
                unittest.mock.call(
                    url="http://127.0.0.1:4985",
                    username=None,
                    password=None,
                ),
            ],
        ),
        (
            ["--sync-gateway-url=http://example.com:4985"],
            [
                unittest.mock.call(
                    url="http://example.com:4985",
                    username=None,
                    password=None,
                ),
                unittest.mock.call(
                    url="http://127.0.0.1:4985",
                    username=None,
                    password=None,
                ),
            ],
        ),
    ],
)
def test_get_sg_url(
    cmdline_args: list[str], expected_calls: list[unittest.mock._Call]
) -> None:
    parser = sgcollect.create_option_parser()
    options, _ = parser.parse_args(cmdline_args)
    with unittest.mock.patch(
        "sgcollect.urlopen_with_basic_auth",
        side_effect=urllib.error.URLError("mock error connecting"),
    ) as mock_urlopen:
        # this URL isn't correct but it is the fallback URL for this function
        assert sgcollect.get_sg_url(options) == "https://127.0.0.1:4985"
        assert mock_urlopen.mock_calls == expected_calls
