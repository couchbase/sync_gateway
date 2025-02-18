# Copyright 2024-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included
# in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
# in that file, in accordance with the Business Source License, use of this
# software will be governed by the Apache License, Version 2.0, included in
# the file licenses/APL2.txt.

import gzip
import json
import os
import pathlib
import sys
import unittest

import password_remover
import pytest
import tasks

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
def test_add_file_task(tmpdir, tag_userdata):
    if tag_userdata:
        expected = REDACTED_OUTPUT
    else:
        expected = REDACTED_OUTPUT.replace("<ud>foo</ud>", "foo")

    filename = "config.json"
    config_file = tmpdir.join(filename)
    config_file.write(INPUT_CONFIG)
    postprocessors = [password_remover.remove_passwords]
    if tag_userdata:
        postprocessors.append(password_remover.tag_userdata_in_server_config)
    task = tasks.add_file_task(
        config_file.strpath,
        content_postprocessors=postprocessors,
    )
    output_dir = tmpdir.mkdir("output")
    runner = tasks.TaskRunner(
        verbosity=VERBOSE,
        default_name="sg.log",
        tmp_dir=output_dir,
    )
    runner.run(task)
    runner.close_all_files()

    with open(pathlib.Path(runner.tmpdir) / filename) as fh:
        assert expected in fh.read()


def test_make_curl_task(tmpdir, httpserver):
    output = "curltask"
    httpserver.expect_request("/").respond_with_json(json.loads(INPUT_CONFIG))
    task = tasks.make_curl_task(
        "curltask",
        httpserver.url_for("/"),
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

    salt = b"AA"
    redactor = tasks.LogRedactor(salt, tmp_path)
    redacted_file = redactor.redact_file(log_file.name, log_file)

    assert False
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
