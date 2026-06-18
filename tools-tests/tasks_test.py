# Copyright 2024-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included
# in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
# in that file, in accordance with the Business Source License, use of this
# software will be governed by the Apache License, Version 2.0, included in
# the file licenses/APL2.txt.

import gzip
import io
import json
import os
import pathlib
import sys
import unittest
import unittest.mock
from zipfile import ZipFile

import password_remover
import pytest
import tasks


def assert_zip_all_nonempty(zip_path: str):
    with ZipFile(zip_path) as zf:
        for info in zf.infolist():
            assert info.file_size > 0, f"{info.filename} is zero-sized in zip"

VERBOSE = 2

INPUT_CONFIG = """\
{
    "password": "password",
    "server": "http://localhost:4984/db",
    "databases": {
        "db" : {
            "users" : {
                "foo" : "bar"
            }
        }
    }
}"""

REDACTED_OUTPUT = """\
{
    "password": "******",
    "server": "http://localhost:4984/db",
    "databases": {
        "db": {
            "users": {
                "<ud>foo</ud>": "bar"
            }
        }
    }
}"""


@pytest.mark.parametrize("tag_userdata", [True, False])
def test_add_file_task(tmp_path, tag_userdata):
    if tag_userdata:
        expected = REDACTED_OUTPUT
    else:
        expected = REDACTED_OUTPUT.replace("<ud>foo</ud>", "foo")

    filename = "config.json"
    config_file = tmp_path / filename
    config_file.write_text(INPUT_CONFIG)
    postprocessors = [password_remover.remove_passwords]
    if tag_userdata:
        postprocessors.append(password_remover.tag_userdata_in_server_config)
    task = tasks.add_file_task(
        sourcefile_path=config_file,
        output_path=config_file.name,
        content_postprocessors=postprocessors,
    )
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    runner = tasks.TaskRunner(
        verbosity=VERBOSE,
        default_name="sg.log",
        tmp_dir=output_dir,
    )
    runner.run(task)
    runner.close_all_files()

    with open(pathlib.Path(runner.tmpdir) / filename) as fh:
        assert expected in fh.read()

    zip_path = str(tmp_path / "out.zip")
    runner.zip(zip_path, "sg", "node1")
    assert_zip_all_nonempty(zip_path)


def test_add_file_task_zero_size(tmp_path):
    filename = "empty.json"
    empty_file = tmp_path / filename
    empty_file.write_text("")
    task = tasks.add_file_task(
        sourcefile_path=empty_file,
        output_path=empty_file.name,
    )
    task.no_header = True
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    runner = tasks.TaskRunner(
        verbosity=VERBOSE,
        default_name="sg.log",
        tmp_dir=output_dir,
    )
    runner.run(task)
    runner.close_all_files()

    zip_path = str(tmp_path / "out.zip")
    runner.zip(zip_path, "sg", "node1")
    with ZipFile(zip_path) as zf:
        assert not any(filename in name for name in zf.namelist())


def test_make_curl_task(tmpdir, httpserver):
    output = "curltask"
    httpserver.expect_request("/").respond_with_json(json.loads(INPUT_CONFIG))
    task = tasks.make_curl_task(
        "curltask",
        httpserver.url_for("/"),
        auth_headers={},
        content_postprocessors=[
            password_remover.remove_passwords,
            password_remover.tag_userdata_in_server_config,
        ],
        log_file=output,
    )

    output_dir = tmpdir.mkdir("output")
    runner = tasks.TaskRunner(
        verbosity=VERBOSE,
        default_name="sg.log",
        tmp_dir=output_dir,
    )
    runner.run(task)
    runner.close_all_files()

    with open(pathlib.Path(runner.tmpdir) / output) as fh:
        assert REDACTED_OUTPUT in fh.read()

    zip_path = str(pathlib.Path(tmpdir) / "out.zip")
    runner.zip(zip_path, "sg", "node1")
    assert_zip_all_nonempty(zip_path)

    httpserver.check()


def test_make_curl_task_zero_size(tmpdir, httpserver):
    output = "curltask_empty"
    httpserver.expect_request("/").respond_with_data("")
    task = tasks.make_curl_task(
        "curltask_empty",
        httpserver.url_for("/"),
        auth_headers={},
        log_file=output,
    )
    task.no_header = True

    output_dir = tmpdir.mkdir("output")
    runner = tasks.TaskRunner(
        verbosity=VERBOSE,
        default_name="sg.log",
        tmp_dir=output_dir,
    )
    runner.run(task)
    runner.close_all_files()

    zip_path = str(pathlib.Path(tmpdir) / "out.zip")
    runner.zip(zip_path, "sg", "node1")
    with ZipFile(zip_path) as zf:
        assert not any(output in name for name in zf.namelist())

    httpserver.check()


def test_task_print_literal(tmp_path):
    task = tasks.AllOsTask("test_task", ["notacommand"], literal="literal")
    runner = tasks.TaskRunner(tmp_dir=tmp_path)
    runner.run(task)
    with open(pathlib.Path(runner.tmpdir) / runner.default_name) as fh:
        assert "literal" in fh.read()


def test_task_timeout(tmp_path):
    task = tasks.AllOsTask("test_task", ["sleep", "5"], timeout=0.01)
    runner = tasks.TaskRunner(tmp_dir=tmp_path)
    runner.run(task)

    with open(pathlib.Path(runner.tmpdir) / runner.default_name) as fh:
        assert "`['sleep', '5']` timed out after 0.01 seconds" in fh.read()


def test_task_popen_exception(tmp_path):
    task = tasks.AllOsTask("test_task", ["notacommand"], timeout=0.01)
    runner = tasks.TaskRunner(tmp_dir=tmp_path)
    with unittest.mock.patch("subprocess.Popen") as popen:
        popen.side_effect = OSError("Boom!")
        runner.run(task)

    with open(pathlib.Path(runner.tmpdir) / runner.default_name) as fh:
        assert "Failed to execute ['notacommand']: Boom!" in fh.read()


@pytest.mark.parametrize("verbosity", [0, 1, 2])
@pytest.mark.parametrize("task_platform", [sys.platform, "fakeplatform"])
def test_task_logging(verbosity, task_platform, tmp_path):
    taskrunner = tasks.TaskRunner(verbosity=verbosity, tmp_dir=tmp_path)
    task = tasks.AllOsTask("echo", "echo")
    # fake the platform for a test
    task.platforms = [task_platform]
    taskrunner.run(task)


@pytest.mark.parametrize(
    "filename,redactable",
    [
        (
            "sync_gateway",
            False,
        ),
        (
            "sync_gateway.exe",
            False,
        ),
        (
            "/abs/path/sync_gateway",
            False,
        ),
        (
            "/abs/path/sync_gateway.exe",
            False,
        ),
        (
            "pprof_heap_high_01.pb.gz",
            False,
        ),
        (
            "pprof.pb",
            False,
        ),
        (
            "/abs/path/pprof.pb",
            False,
        ),
        (
            "sg_info.log",
            True,
        ),
        (
            "sg_info-01.log.gz",
            True,
        ),
        (
            "/abs/path/sg_info.log",
            True,
        ),
        (
            "/abs/path/sg_info-01.log.gz",
            True,
        ),
        (
            "expvars.json",
            False,
        ),
        (
            "/abs/path/expvars.json",
            False,
        ),
    ],
)
@pytest.mark.parametrize("use_pathlib", [True, False])
def test_redactable_filename(use_pathlib, filename, redactable):
    if use_pathlib:
        filename = pathlib.Path(filename)
    assert tasks.redactable_file(filename) is redactable


def test_log_redact_file(tmp_path):
    log_file = tmp_path / "foo.log.gz"
    input_log_lines = [
        "logline1: foo",
        "logline2: <ud>password</ud>",
        "logline3: log-redaction-salt=AAA",
        "logline4: bar",
    ]
    with gzip.open(log_file, "wt") as fh:
        for line in input_log_lines:
            fh.write(line + "\n")

    salt = "AA"
    redactor = tasks.LogRedactor(salt, tmp_path)
    redacted_file = redactor.redact_file(log_file.name, log_file)

    output_log_lines = [
        "RedactLevel:partial,HashOfSalt:e2512172abf8cc9f67fdd49eb6cacf2df71bbad3",
        "logline1: foo",
        "logline2: <ud>1700bc8ae71605063ae83d80837fa53988c635ef</ud>",
        "logline3: log-redaction-salt <redacted>",
        "logline4: bar",
        "",  # file has trailing newline
    ]
    updated_text = os.linesep.join(output_log_lines).encode("utf-8")

    redacted_text = gzip.open(redacted_file).read()
    assert redacted_text == updated_text


# ---------------------------------------------------------------------------
# PythonTask.execute() — direct return-value tests
# ---------------------------------------------------------------------------


@pytest.fixture
def python_task_fp():
    """A BytesIO buffer standing in for the log file descriptor."""
    return io.BytesIO()


def _make_python_task(callable, **kwargs):
    return tasks.PythonTask("my task", callable, log_file="out.log", **kwargs)


def test_python_task_execute_no_output(python_task_fp):
    task = _make_python_task(lambda: b"")
    exit_code, message = task.execute(python_task_fp)
    assert exit_code == 1
    assert message == "task my task had no output"
    assert python_task_fp.getvalue() == b""


def test_python_task_execute_bytes(python_task_fp):
    task = _make_python_task(lambda: b"hello")
    exit_code, message = task.execute(python_task_fp)
    assert exit_code == 0
    assert message == ""
    assert python_task_fp.getvalue() == b"hello"


def test_python_task_execute_str(python_task_fp):
    task = _make_python_task(lambda: "hello")
    exit_code, message = task.execute(python_task_fp)
    assert exit_code == 0
    assert message == ""
    assert python_task_fp.getvalue() == b"hello"


def test_python_task_execute_exception_silent(python_task_fp):
    task = _make_python_task(lambda: (_ for _ in ()).throw(ValueError("boom")), log_exception=False)
    exit_code, message = task.execute(python_task_fp)
    assert exit_code == 1
    assert message == ""


def test_python_task_execute_exception_logged(python_task_fp, capsys):
    task = _make_python_task(lambda: (_ for _ in ()).throw(ValueError("boom")), log_exception=True)
    exit_code, message = task.execute(python_task_fp)
    assert exit_code == 1
    assert message == ""
    assert "Exception executing python task: boom" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# TaskRunner.log_result() — stderr output
# ---------------------------------------------------------------------------


def test_log_result_ok(tmp_path, capsys):
    runner = tasks.TaskRunner(tmp_dir=tmp_path)
    runner.log_result((0, ""))
    assert "OK" in capsys.readouterr().err


def test_log_result_with_message(tmp_path, capsys):
    runner = tasks.TaskRunner(tmp_dir=tmp_path)
    runner.log_result((1, "task my task had no output"))
    assert "Exit code 1: task my task had no output" in capsys.readouterr().err


def test_log_result_no_message(tmp_path, capsys):
    runner = tasks.TaskRunner(tmp_dir=tmp_path)
    runner.log_result((127, ""))
    assert "Exit code 127" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# Task.execute() — return-code assertions
# ---------------------------------------------------------------------------


def test_task_execute_literal_return():
    task = tasks.AllOsTask("test", ["notacommand"], literal="hello")
    exit_code, message = task.execute(io.BytesIO())
    assert exit_code == 0
    assert message == ""


def test_task_execute_popen_exception_return():
    task = tasks.AllOsTask("test", ["notacommand"])
    with unittest.mock.patch("subprocess.Popen") as popen:
        popen.side_effect = OSError("Boom!")
        exit_code, message = task.execute(io.BytesIO())
    assert exit_code == 127
    assert message == ""


def test_task_execute_timeout_return():
    task = tasks.AllOsTask("test", ["sleep", "5"], timeout=0.01)
    exit_code, message = task.execute(io.BytesIO())
    assert exit_code != 0
    assert message == ""
