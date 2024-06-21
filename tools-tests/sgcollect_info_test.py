import sgcollect_info
import unittest
import io


def test_make_collect_logs_tasks(tmpdir):
    with unittest.mock.patch(
        "sgcollect_info.urlopen_with_basic_auth",
        return_value=io.BytesIO(f'{{"logfilepath": "{tmpdir}"}}'.encode("utf-8")),
    ):
        log_file = tmpdir.join("sg_info.log")
        log_file.write("foo")
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
