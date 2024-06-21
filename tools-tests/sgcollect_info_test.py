import unittest
import io

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
            config.format(tmpdir=tmpdir, log_file=log_file).encode("utf-8")
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
        assert len(tasks) == 2
        assert [t.log_file for t in tasks] == [
            log_file.basename,
            rotated_log_file.basename,
        ]
