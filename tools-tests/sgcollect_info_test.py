# Copyright 2024-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included
# in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
# in that file, in accordance with the Business Source License, use of this
# software will be governed by the Apache License, Version 2.0, included in
# the file licenses/APL2.txt.

import io
import unittest

import pytest
import sgcollect_info


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
            tmpdir,
            sg_url="fakeurl",
            sg_config_file_path="",
            sg_username="",
            sg_password="",
            salt="",
            should_redact=False,
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
            tmpdir,
            sg_url="fakeurl",
            sg_config_file_path="",
            sg_username="",
            sg_password="",
            salt="",
            should_redact=False,
        )
        assert [tasks[0].log_file] == [pprof_file.basename]
