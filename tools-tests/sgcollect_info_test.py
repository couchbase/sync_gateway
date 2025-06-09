# Copyright 2024-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included
# in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
# in that file, in accordance with the Business Source License, use of this
# software will be governed by the Apache License, Version 2.0, included in
# the file licenses/APL2.txt.

import io
import pathlib
import unittest

import pytest
import sgcollect_info
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
        "sgcollect_info.urlopen_with_basic_auth",
        return_value=io.BytesIO(
            config.format(
                tmpdir=str(tmpdir).replace("\\", "\\\\"),
                log_file=str(log_file).replace("\\", "\\\\"),
            ).encode("utf-8")
        ),
    ):
        rotated_log_file = tmpdir.join("sg_info-01.log.gz")
        rotated_log_file.write("foo")
        tasks = sgcollect_info.make_collect_logs_tasks(
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
        "sgcollect_info.urlopen_with_basic_auth",
        return_value=io.BytesIO(
            '{{"logfilepath": "{logpath}"}}'.format(
                logpath=str(tmpdir).replace("\\", "\\\\")
            ).encode("utf-8")
        ),
    ):
        pprof_file = tmpdir.join("pprof_heap_high_01.pb.gz")
        pprof_file.write("foo")
        tasks = sgcollect_info.make_collect_logs_tasks(
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
        "sgcollect_info.urlopen_with_basic_auth",
        return_value=io.BytesIO(
            config.format(
                tmpdir1=str(tmpdir1).replace("\\", "\\\\"),
                tmpdir2=str(tmpdir2).replace("\\", "\\\\"),
            ).encode("utf-8")
        ),
    ):
        tasks = sgcollect_info.make_collect_logs_tasks(
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
    assert sgcollect_info.get_unique_filename(basenames, filename) == expected

@pytest.mark.parametrize("cmdline", [
    ([]),
    (["--log-redaction-level", "full"]),
    (["--log-redaction-level", "none"]),
    (["--log-redaction-l", "none"]),
    (["--sync-gateway-password=mypassword"]),
    (["--sync-gateway-pa=mypassword"]),
    (["--sync-gateway-password", "mypassword"]),
 ])
def test_get_sgcollect_info_options_task(tmp_path, cmdline):
    runner = tasks.TaskRunner(tmp_dir=tmp_path)
    parser = sgcollect_info.create_option_parser()
    options, args = parser.parse_args(cmdline + ["fakesgcollect.zip"])
    task = sgcollect_info.get_sgcollect_info_options_task(options, args)
    runner.run(task)

    output = (
        pathlib.Path(runner.tmpdir) / sgcollect_info.SGCOLLECT_INFO_OPTIONS_LOG
    ).read_text()
    assert "sync_gateway_password" not in output
    assert f"args: {args}" in output
