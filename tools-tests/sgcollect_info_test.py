import sgcollect_info
import unittest
import io


def test_make_collect_logs_tasks(tmpdir):
    with unittest.mock.patch(
        "sgcollect_info.urlopen_with_basic_auth",
        return_value=io.BytesIO(
            '{{"logfilepath": "{logdir}"}}'.format(
                logdir=str(tmpdir).replace(
                    "\\", "\\\\"
                )  # replace backslashes with double backslashes for windows paths in json
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
