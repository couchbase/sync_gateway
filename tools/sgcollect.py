#!/usr/bin/env python3

"""
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
"""

# -*- python -*-
import base64
import getpass
import glob
import json
import optparse
import os
import pathlib
import platform
import re
import shutil
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid
from typing import List, Literal, NoReturn, Optional, Union

import password_remover
from tasks import (
    AllOsTask,
    CbcollectInfoOptions,
    PythonTask,
    TaskRunner,
    add_file_task,
    do_upload,
    flatten,
    generate_upload_url,
    log,
    make_curl_task,
    make_os_tasks,
    urlopen,
)

try:
    # Don't validate HTTPS by default.
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    # Running an older version of Python which won't validate HTTPS anyway.
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

# Collects the following info from Sync Gateway
#
# - System Stats (top, netstat, etc)
# - Sync Gateway logs
# - Expvar Json
# - pprof files (profiling / memory)
# - Startup and running SG config
#
# See https://github.com/couchbase/sync_gateway/issues/1640
#
# Python version compatibility:
#
# Until the libc / centos6 issues are resolved, this should remain python 2.6 compatible.
# One common incompatibility is the formatting syntax, as discussed here: http://bit.ly/2rIH8wg
USAGE = """usage: %prog [options] output_file.zip

    %prog --sync-gateway-url=https://127.0.0.1:4985 --sync-gateway-username=SYNC_GATEWAY_ADMIN_USERNAME output_file.zip
    %prog --sync-gateway-url=https://127.0.0.1:4985 --sync-gateway-username=SYNC_GATEWAY_ADMIN_USERNAME -v output_file.zip"""

DESCRIPTION = """Collect information from Sync Gateway for support purposes. To collect usable information, you must provide a username that has access to the Sync Gateway Admin API via --sync-gateway-username or SG_USERNAME environment variable. The Sync Gateway Admin API password will be prompted for if not provided via environment variable SG_PASSWORD."""
mydir = os.path.dirname(sys.argv[0])

# name of the log file that contains the options passed to sgcollect_info
SGCOLLECT_INFO_OPTIONS_LOG = "sgcollect_info_options.log"

SG_USERNAME_ENV = "SG_USERNAME"
SG_PASSWORD_ENV = "SG_PASSWORD"
SG_PASSWORD_ERROR = f"--sync-gateway-password is no longer functional. --sync-gateway-username will ask for an interactive password or use environment variable {SG_PASSWORD_ENV}=<password>."


def delete_zip(filename):
    """
    Deletes file if exists. Does not raise an exception in any circumstance.
    """
    try:
        os.remove(filename)
        print("Zipfile deleted: {0}".format(filename))
    except FileNotFoundError:
        pass
    except Exception as e:
        print("Zipfile ({0}) deletion failed: {1}".format(filename, e))


def sync_gateway_option_callback(
    option: optparse.Option,
    opt: str,
    value: str,
    parser: optparse.OptionParser,
) -> None:
    if value:
        raise optparse.OptionValueError(SG_PASSWORD_ERROR)


class HelpFormatter(optparse.IndentedHelpFormatter):
    """
    HelpFormatter that does not truncate help to 80 characters, matches argparse implementation.
    """

    def __init__(
        self,
        indent_increment: int = 2,
        max_help_position: int = 24,
        width: Optional[int] = None,
        short_first: Union[bool, Literal[0, 1]] = 1,
    ):
        # match width from argparse implementation
        width = shutil.get_terminal_size().columns - 2
        super().__init__(indent_increment, max_help_position, width, short_first)


def create_option_parser():
    parser = optparse.OptionParser(
        usage=USAGE,
        option_class=CbcollectInfoOptions,
        description=DESCRIPTION,
        formatter=HelpFormatter(),
    )
    parser.add_option(
        "--sync-gateway-url",
        dest="sync_gateway_url",
        help="Sync Gateway admin port URL. By default, will try https://127.0.0.1:4985 and http://127.0.0.1:4984",
    )
    parser.add_option(
        "--sync-gateway-username",
        dest="sync_gateway_username",
        help="Sync Gateway Admin API username. This is required to collect logging and configuration information. Can be specified with SG_USERNAME env variable.",
    )

    # -r is unused, copied from cbcollect_info
    parser.add_option(
        "-r",
        dest="root",
        help=optparse.SUPPRESS_HELP,
        default=os.path.abspath(os.path.join(mydir, "..")),
    )
    parser.add_option(
        "--log-redaction-level",
        dest="redact_level",
        default="none",
        help="redaction level for the logs collected, none and partial supported (default is none)",
    )
    parser.add_option(
        "--log-redaction-salt",
        dest="salt_value",
        default=str(uuid.uuid4()),
        help="Is used to salt the hashing of tagged data, "
        "defaults to random uuid. If input by user it should "
        "be provided along with --log-redaction-level option",
    )
    parser.add_option(
        "--just-upload-into", dest="just_upload_into", help=optparse.SUPPRESS_HELP
    )
    parser.add_option(
        "--upload-host",
        dest="upload_host",
        help="if specified, gathers diagnostics and uploads it to the specified host,"
        " e.g 'https://uploads.couchbase.com'",
    )
    parser.add_option(
        "--customer",
        dest="upload_customer",
        help="used in conjunction with '--upload-host' and '--ticket', "
        "specifies the customer name for the upload",
    )
    parser.add_option(
        "--ticket",
        dest="upload_ticket",
        type="ticket",
        help="used in conjunction with '--upload-host' and '--customer',"
        " specifies the support ticket number for the upload."
        " e.g 1234 (must be numeric), contact Couchbase Support to open a new"
        " ticket if you do not already have one.  For more info, see"
        " https://www.couchbase.com/wiki/display/couchbase/Working+with+the+Couchbase+Technical+Support+Team",
    )
    parser.add_option(
        "--sync-gateway-config",
        dest="sync_gateway_config",
        help="path to Sync Gateway config. If able to connect to Sync Gateway Admin API, this does not need to be specified.",
    )
    parser.add_option(
        "--sync-gateway-executable",
        dest="sync_gateway_executable",
        help="path to Sync Gateway executable. If able to connect to Sync Gateway Admin API, this does not need to be specified.",
    )
    parser.add_option(
        "--sync-gateway-password",
        action="callback",
        callback=sync_gateway_option_callback,
        dest="do_not_use",
        type=str,
        help=optparse.SUPPRESS_HELP,
    )
    parser.add_option(
        "--upload-proxy",
        dest="upload_proxy",
        default="",
        help="specifies proxy for upload",
    )
    parser.add_option(
        "--tmp-dir",
        dest="tmp_dir",
        default=None,
        help="set the temp dir used while processing collected data. Overrides the TMPDIR env variable if set",
    )
    parser.add_option(
        "--keep-zip",
        dest="keep_zip",
        action="store_true",
        default=False,
        help="keep sgcollect zip files after uploading them. By default they will be deleted to save disk space",
    )
    parser.add_option(
        "-p",
        dest="product_only",
        help="gather only product related information",
        action="store_true",
        default=False,
    )
    parser.add_option(
        "-v",
        dest="verbosity",
        help="increase verbosity level",
        action="count",
        default=0,
    )

    return parser


def expvar_url(sg_url):
    return "{0}/_expvar".format(sg_url)


def make_http_client_pprof_tasks(
    sg_url: str, auth_headers: dict[str, str]
) -> list[PythonTask]:
    """
    These tasks use the python http client to collect the raw pprof data, which can later
    be rendered into something human readable
    """
    profile_types = [
        "profile",
        "heap",
        "goroutine",
        "block",
        "mutex",
    ]

    base_pprof_url = "{0}/_debug/pprof".format(sg_url)

    pprof_tasks = []
    for profile_type in profile_types:
        sg_pprof_url = "{0}/{1}".format(base_pprof_url, profile_type)
        clean_task = make_curl_task(
            name="Collect {0} pprof via http client".format(profile_type),
            auth_headers=auth_headers,
            url=sg_pprof_url,
            log_file="pprof_{0}.pb.gz".format(profile_type),
        )
        clean_task.no_header = True
        pprof_tasks.append(clean_task)

    return pprof_tasks


def make_http_client_stack_trace_task(
    sg_url: str, auth_headers: dict[str, str]
) -> PythonTask:
    """
    This task uses the python http client to collect the raw stack trace data
    """
    stack_trace_url = "{0}/_debug/pprof/goroutine?debug=2".format(sg_url)

    stack_trace_task = make_curl_task(
        name="Collect stack trace via http client",
        auth_headers=auth_headers,
        url=stack_trace_url,
        log_file="sg_stack_trace.log",
    )
    stack_trace_task.no_header = True

    return stack_trace_task


def to_lower_case_keys_dict(original_dict):
    result = {}
    for k, v in list(original_dict.items()):
        result[k.lower()] = v
    return result


def extract_element_from_config(element, config):
    """The config returned from /_config may not be fully formed json
    due to the fact that the sync function is inside backticks (`)
    Therefore this method grabs an element from the config after
    removing the sync function
    """

    sync_regex = r'"Sync":(`.*`)'
    config = re.sub(sync_regex, '"Sync":""', config)
    try:
        # convert dictionary keys to lower case
        original_dict = json.loads(config)
        lower_case_keys_dict = to_lower_case_keys_dict(original_dict)

        # lookup key after converting element name to lower case
        return lower_case_keys_dict[element.lower()]
    except (ValueError, KeyError):
        # If we can't deserialize the json or find the key then return nothing
        return


def extract_element_from_default_logging_config(element, config):
    # extracts a property from nested logging object
    try:
        logging_config = extract_element_from_config("Logging", config)
        if logging_config:
            default_logging_config = extract_element_from_config(
                "default", json.dumps(logging_config)
            )
            if default_logging_config:
                guessed_log_path = extract_element_from_config(
                    element, json.dumps(default_logging_config)
                )
                if guessed_log_path:
                    return guessed_log_path
        return
    except (ValueError, KeyError):
        # If we can't deserialize the json or find the key then return nothing
        return


def extract_element_from_logging_config(element, config):
    # extracts a property from nested logging object
    try:
        logging_config = extract_element_from_config("logging", config)
        if logging_config:
            guessed_log_path = extract_element_from_config(
                element, json.dumps(logging_config)
            )
            if guessed_log_path:
                return guessed_log_path
        return
    except (ValueError, KeyError):
        # If we can't deserialize the json or find the key then return nothing
        return


def get_unique_filename(filenames: set[str], original_filename: str) -> str:
    """
    Given a set of filenames, return a unique filename that is not in the set.
    """
    i = 1
    filename = original_filename
    while filename in filenames:
        filename = f"{original_filename}.{i}"
        i += 1
    return filename


def make_collect_logs_tasks(
    sg_url: str,
    sg_config_file_path: Optional[str],
    auth_headers: dict[str, str],
) -> List[PythonTask]:
    sg_log_files = {
        "sg_error.log": "sg_error.log",
        "sg_warn.log": "sg_warn.log",
        "sg_info.log": "sg_info.log",
        "sg_debug.log": "sg_debug.log",
        "sg_trace.log": "sg_trace.log",
        "sg_stats.log": "sg_stats.log",
        "sg_stack_trace.log": "sg_stack_trace.log",
        "sync_gateway_access.log": "sync_gateway_access.log",
        "sync_gateway_error.log": "sync_gateway_error.log",
        "pprof.pb": "pprof.pb",
    }

    os_home_dirs = [
        "/home/sync_gateway/logs",
        "/var/log/sync_gateway",
        "/Users/sync_gateway/logs",  # OSX sync gateway
        R"C:\Program Files (x86)\Couchbase\var\lib\couchbase\logs",  # Windows (Pre-2.0)
        R"C:\Program Files\Couchbase\var\lib\couchbase\logs",  # Windows (Post-2.0)
        R"C:\Program Files\Couchbase\Sync Gateway\var\lib\couchbase\logs",  # Windows (Post-2.1) sync gateway
    ]
    # Try to find user-specified log path
    if sg_url:
        config_url = "{0}/_config?include_runtime=true".format(sg_url)
        try:
            response = urlopen(config_url, auth_headers)
        except urllib.error.URLError:
            print("Failed to load SG config from running SG.")
            config_str = ""
        else:
            config_str = response.read().decode("utf-8")
    else:
        config_str = ""

    # If SG isn't running, load the bootstrap config - it'll have the logging config in it
    if (
        config_str == ""
        and sg_config_file_path is not None
        and sg_config_file_path != ""
    ):
        try:
            with open(sg_config_file_path) as fd:
                print("Loading SG config from path on disk.")
                config_str = fd.read()
        except Exception as e:
            print("Failed to load SG config from disk: {0}".format(e))

    # Find log file path from old style top level config
    guessed_log_path = extract_element_from_config("LogFilePath", config_str)
    if guessed_log_path and os.path.isfile(guessed_log_path):
        # If the specified log path is a file, add filename to standard SG log files list
        # and parent directory path of the file to standard SG log directories list to
        # eventually look for all permutations of SG log files and directories.
        os_home_dirs.append(os.path.dirname(guessed_log_path))
        sg_log_files[os.path.basename(guessed_log_path)] = os.path.basename(
            guessed_log_path
        )
    elif guessed_log_path and os.path.isdir(guessed_log_path):
        # If the specified log path is a directory, add that path to the standard
        # SG log directories list to eventually look for the standard SG log files.
        os_home_dirs.append(guessed_log_path)

    # Keep a dictionary of log file paths we've added, to avoid adding duplicates
    sg_log_file_paths: set[pathlib.Path] = set()
    output_basenames: set[str] = set()

    sg_tasks: List[PythonTask] = []

    def add_file_collection_task(filename: str):
        canonical_filename = pathlib.Path(filename)
        canonical_filename.resolve()
        if canonical_filename in sg_log_file_paths:
            return
        output_basename = get_unique_filename(output_basenames, canonical_filename.name)
        sg_log_file_paths.add(canonical_filename)
        output_basenames.add(output_basename)
        task = add_file_task(sourcefile_path=filename, output_path=output_basename)
        task.no_header = True
        sg_tasks.append(task)

    def lookup_std_log_files(files, dirs):
        for dir in dirs:
            for file in files:
                name, ext = os.path.splitext(file)
                # Collect active and rotated log files from the default log locations.
                pattern_rotated = os.path.join(dir, "{0}*{1}".format(name, ext))
                for std_log_file in glob.glob(pattern_rotated):
                    add_file_collection_task(std_log_file)

                # Collect archived log files from the default log locations.
                pattern_archived = os.path.join(dir, "{0}*{1}.gz".format(name, ext))
                for std_log_file in glob.glob(pattern_archived):
                    add_file_collection_task(std_log_file)

    # Lookup each standard SG log files in each standard SG log directories.
    lookup_std_log_files(sg_log_files, os_home_dirs)

    # Find log file path from logging.["default"] style config
    # When the log file path from the default style config is a directory, we need to look for all
    # standard SG log files inside the directory including rotated log files. But that handling is
    # not included here, it relies on the fall through to the 2.1 style handling to pick up those
    # log files that were written to the directory path instead; SG exposes the log_file_path as
    # the parent directory of the file specified against LogFilePath through /_config endpoint.
    guessed_logging_path = extract_element_from_default_logging_config(
        "LogFilePath", config_str
    )
    if guessed_logging_path and os.path.isfile(guessed_logging_path):
        # Get the parent directory and the log file name
        log_file_parent_dir = os.path.abspath(
            os.path.join(guessed_logging_path, os.pardir)
        )
        log_file_name = os.path.basename(guessed_logging_path)
        name, ext = os.path.splitext(log_file_name)

        # Lookup SG log files inside the parent directory, including rotated log files.
        rotated_logs_pattern = os.path.join(
            log_file_parent_dir, "{0}*{1}".format(name, ext)
        )
        for log_file_item_name in glob.iglob(rotated_logs_pattern):
            log_file_item_path = os.path.join(log_file_parent_dir, log_file_item_name)
            add_file_collection_task(log_file_item_path)

        # Lookup standard SG log files inside the parent directory.
        lookup_std_log_files(sg_log_files, [log_file_parent_dir])

    # Find log file path from SGW 2.1 style logging config
    guessed_log_file_path = extract_element_from_logging_config(
        "log_file_path", config_str
    )
    if guessed_log_file_path:
        log_file_path = os.path.abspath(guessed_log_file_path)

        for log_file_name in sg_log_files:
            # iterate over all log files, including those with a rotation timestamp
            # e.g: sg_info-2018-12-31T13-33-41.055.log
            name, ext = os.path.splitext(log_file_name)
            log_file_pattern = "{0}*{1}".format(name, ext)
            rotated_logs_pattern = os.path.join(log_file_path, log_file_pattern)

            for log_file_item_name in glob.iglob(rotated_logs_pattern):
                log_file_item_path = os.path.join(log_file_path, log_file_item_name)
                add_file_collection_task(log_file_item_path)

            # try gzipped logs too
            # e.g: sg_info-2018-12-31T13-33-41.055.log.gz
            log_file_pattern = "{0}*{1}.gz".format(name, ext)
            rotated_logs_pattern = os.path.join(log_file_path, log_file_pattern)

            for log_file_item_name in glob.iglob(rotated_logs_pattern):
                log_file_item_path = os.path.join(log_file_path, log_file_item_name)
                add_file_collection_task(log_file_item_path)

    return sg_tasks


def get_db_list(sg_url: str, auth_headers: dict[str, str]) -> list[str]:
    # build url to _all_dbs
    all_dbs_url = "{0}/_all_dbs".format(sg_url)
    data: list[str] = []

    # get content and parse into json
    try:
        response = urlopen(all_dbs_url, auth_headers)
        data = json.load(response)
    except urllib.error.URLError as e:
        print(f"WARNING: Unable to connect to Sync Gateway: {sg_url} {e}")

    # return list of dbs
    return data


# Startup config
#   Commandline args (covered in expvars, IIRC)
#   json file.
# Running config
#   Server config
#   Each DB config
def make_config_tasks(
    sg_config_path: Optional[str],
    sg_url: str,
    auth_headers: dict[str, str],
    should_redact: bool,
) -> List[PythonTask]:
    """
    Return a list of tasks suitable for collecting configuration information.

    1. sync gateway configuration file.
    2. /_config
    3. /_config?include_runtime=true
    4. Each /<db>/_config
    5. /_cluster_info
    """
    collect_config_tasks: list[PythonTask] = []

    # Here are the "usual suspects" to probe for finding the static config
    sg_config_files = [
        "/home/sync_gateway/sync_gateway.json",  # linux sync gateway
        "/opt/sync_gateway/etc/sync_gateway.json",  # amazon linux AMI sync gateway
        "/Users/sync_gateway/sync_gateway.json"  # OSX sync gateway
        R"C:\Program Files (x86)\Couchbase\serviceconfig.json"  # Windows (Pre-2.0) sync gateway
        R"C:\Program Files\Couchbase\Sync Gateway\serviceconfig.json",  # Windows (Post-2.0) sync gateway
    ]
    sg_config_files = [x for x in sg_config_files if os.path.exists(x)]

    # If a config path was discovered from the expvars, or passed in via the user, add that in the
    # list of files to probe
    if sg_config_path is not None:
        sg_config_files.append(sg_config_path)

    # Tag user data before redaction, if redact_level is set
    server_config_postprocessors = [password_remover.remove_passwords]
    db_config_postprocessors = [password_remover.remove_passwords]
    if should_redact:
        server_config_postprocessors.append(
            password_remover.tag_userdata_in_server_config
        )
        db_config_postprocessors.append(password_remover.tag_userdata_in_db_config)

    # Get static server config
    for sg_config_file in sg_config_files:
        task = add_file_task(
            sourcefile_path=sg_config_file,
            output_path=os.path.basename(sg_config_file),
            content_postprocessors=server_config_postprocessors,
        )
        collect_config_tasks.append(task)

    # Get server config
    server_config_url = "{0}/_config".format(sg_url)

    config_task = make_curl_task(
        name="Collect server config",
        auth_headers=auth_headers,
        url=server_config_url,
        log_file="sync_gateway.log",
        content_postprocessors=server_config_postprocessors,
    )
    collect_config_tasks.append(config_task)

    # Get server config with runtime defaults and runtime dbconfigs
    server_runtime_config_url = "{0}/_config?include_runtime=true".format(sg_url)
    runtime_config_task = make_curl_task(
        name="Collect runtime config",
        auth_headers=auth_headers,
        url=server_runtime_config_url,
        log_file="sync_gateway.log",
        content_postprocessors=server_config_postprocessors,
    )
    collect_config_tasks.append(runtime_config_task)

    # Get persisted dbconfigs
    dbs = get_db_list(sg_url, auth_headers)
    for db in dbs:
        db_config_url = "{0}/{1}/_config".format(sg_url, db)
        db_config_task = make_curl_task(
            name="Collect {0} database config".format(db),
            auth_headers=auth_headers,
            url=db_config_url,
            log_file="sync_gateway.log",
            content_postprocessors=db_config_postprocessors,
        )
        collect_config_tasks.append(db_config_task)

    # Get cluster information
    cluster_info_url = "{0}/_cluster_info".format(sg_url)
    cluster_info_task = make_curl_task(
        name="Collect SG cluster info",
        auth_headers=auth_headers,
        url=cluster_info_url,
        log_file="sync_gateway.log",
        content_postprocessors=server_config_postprocessors,
    )
    collect_config_tasks.append(cluster_info_task)

    return collect_config_tasks


def get_config_path_from_cmdline(cmdline_args: list[str]) -> Optional[str]:
    """
    Parse command line arguments to find the configuration file path and return an absolute path. May return None if the path doesn't exist.

    Example input::

        sync_gateway -json config.json

    """
    for cmdline_arg in cmdline_args:
        # if it has .json in the path, assume it's a config file.
        # ignore any config files that are URL's for now, since
        # they won't be handled correctly.
        if ".json" in cmdline_arg and "http" not in cmdline_arg:
            return str(pathlib.Path(cmdline_arg).resolve())
    return None


def get_paths_from_expvars(
    sg_url: str, auth_headers: dict[str, str]
) -> tuple[Optional[str], Optional[str]]:
    """
    Get the Sync Gateway binary and configuration file path from /_expvar endpoint.
    """
    data = None
    sg_binary_path = None
    sg_config_path = None

    # get content and parse into json
    if not sg_url:
        return None, None

    try:
        response = urlopen(expvar_url(sg_url), auth_headers)
    except urllib.error.URLError as e:
        print(f"WARNING: Unable to connect to Sync Gateway: {sg_url} {e}")
        return None, None
    try:
        data = json.load(response)
    except json.JSONDecodeError as e:
        print(f"WARNING: Unable to deserialize expvar output: {e}")
        return None, None

    if data is not None and "cmdline" in data:
        cmdline_args = data["cmdline"]
        if len(cmdline_args) == 0:
            return (sg_binary_path, sg_config_path)
        sg_binary_path = cmdline_args[0]
        if len(cmdline_args) > 1:
            sg_config_path = get_config_path_from_cmdline(cmdline_args[1:])

    return (sg_binary_path, sg_config_path)


def make_download_expvars_task(sg_url: str, auth_headers: dict[str, str]) -> PythonTask:
    task = make_curl_task(
        name="download_sg_expvars",
        auth_headers=auth_headers,
        url=expvar_url(sg_url),
        log_file="expvars.json",
    )

    task.no_header = True

    return task


def make_sg_tasks(
    *,
    sg_url: str,
    auth_headers: dict[str, str],
    sync_gateway_config_path_option: Optional[str],
    sync_gateway_executable_path: Optional[str],
    should_redact: bool,
) -> List[PythonTask]:
    # Get path to sg binary (reliable) and config (not reliable)
    sg_binary_path, sg_config_path = get_paths_from_expvars(
        sg_url,
        auth_headers=auth_headers,
    )
    print(
        "Discovered from expvars: sg_binary_path={0} sg_config_path={1}".format(
            sg_binary_path, sg_config_path
        )
    )

    # If user passed in a specific path to the SG binary, then use it
    if (
        sync_gateway_executable_path is not None
        and len(sync_gateway_executable_path) > 0
    ):
        if not os.path.exists(sync_gateway_executable_path):
            raise Exception(
                "Path to sync gateway executable passed in does not exist: {0}".format(
                    sync_gateway_executable_path
                )
            )
        sg_binary_path = sync_gateway_executable_path

    if (
        sg_config_path is None
        and sync_gateway_config_path_option is not None
        and len(sync_gateway_config_path_option) > 0
    ):
        sg_config_path = sync_gateway_config_path_option

    # Collect logs
    collect_logs_tasks = make_collect_logs_tasks(
        sg_url,
        sg_config_path,
        auth_headers=auth_headers,
    )

    py_expvar_task = make_download_expvars_task(sg_url, auth_headers)

    # If the user passed in a valid config path, then use that rather than what's in the expvars
    if (
        sync_gateway_config_path_option is not None
        and len(sync_gateway_config_path_option) > 0
        and os.path.exists(sync_gateway_config_path_option)
    ):
        sg_config_path = sync_gateway_config_path_option

    http_client_pprof_tasks = make_http_client_pprof_tasks(sg_url, auth_headers)

    http_client_stack_trace_task = make_http_client_stack_trace_task(
        sg_url, auth_headers
    )

    # Add a task to collect Sync Gateway config
    config_tasks = make_config_tasks(
        sg_config_path, sg_url, auth_headers, should_redact
    )

    # Curl the /_status
    status_tasks = make_curl_task(
        name="Collect server status",
        auth_headers=auth_headers,
        url="{0}/_status".format(sg_url),
        log_file="sync_gateway.log",
        content_postprocessors=[password_remover.pretty_print_json],
    )

    # Combine all tasks into flattened list
    sg_tasks = flatten(
        [
            collect_logs_tasks,
            py_expvar_task,
            http_client_pprof_tasks,
            http_client_stack_trace_task,
            config_tasks,
            status_tasks,
        ]
    )

    return sg_tasks


def discover_sg_binary_path(
    options: optparse.Values,
    sg_url: Optional[str],
    auth_headers: dict[str, str],
) -> str:
    """
    Return the path to the sync gateway binary, returns None if the path is not found.

    1. --sync-gateway-executable option, will through an exception if the path does not exist.
    2. If the expvars endpoint is available, it will try to get the path from there.
    3. Discover the path from a set of common locations.
    """
    if options.sync_gateway_executable:
        if not os.path.exists(options.sync_gateway_executable):
            raise Exception(
                "Path to sync gateway executable passed in does not exist: {0}".format(
                    options.sync_gateway_executable
                )
            )
        return options.sync_gateway_executable
    if sg_url:
        sg_binary_path, _ = get_paths_from_expvars(sg_url, auth_headers=auth_headers)
        if sg_binary_path:
            return sg_binary_path
    sg_bin_dirs = [
        "/opt/couchbase-sync-gateway/bin/sync_gateway",  # Linux + OSX
        R"C:\Program Files (x86)\Couchbase\sync_gateway.exe",  # Windows (Pre-2.0)
        R"C:\Program Files\Couchbase\Sync Gateway\sync_gateway.exe",  # Windows (Post-2.0)
    ]

    for sg_binary_path_candidate in sg_bin_dirs:
        if os.path.exists(sg_binary_path_candidate):
            return sg_binary_path_candidate
    return ""


def main() -> NoReturn:
    # ask all tools to use C locale (MB-12050)
    os.environ["LANG"] = "C"
    os.environ["LC_ALL"] = "C"

    # Workaround MB-8239: erl script fails in OSX as it is unable to find COUCHBASE_TOP
    if platform.system() == "Darwin":
        os.environ["COUCHBASE_TOP"] = os.path.abspath(os.path.join(mydir, ".."))

    # Parse command line options
    parser = create_option_parser()
    options, args = parser.parse_args()

    # Validate args
    if len(args) != 1:
        parser.error(
            "incorrect number of arguments. Expecting filename to collect diagnostics into"
        )

    auth_headers = get_auth_headers(
        options.sync_gateway_username,
    )
    sg_url = get_sg_url(options, auth_headers)

    # Build path to zip directory, make sure it exists
    zip_filename = args[0]
    if zip_filename[-4:] != ".zip":
        zip_filename = zip_filename + ".zip"
    zip_dir = os.path.dirname(os.path.abspath(zip_filename))
    if not os.access(zip_dir, os.W_OK | os.X_OK):
        print("do not have write access to the directory %s" % (zip_dir))
        sys.exit(1)

    if options.redact_level != "none" and options.redact_level != "partial":
        parser.error(
            "Invalid redaction level. Only 'none' and 'partial' are supported."
        )

    should_redact = should_redact_from_options(options)
    redact_zip_file = zip_filename[:-4] + "-redacted" + zip_filename[-4:]
    if should_redact:
        # Generate the s3 URL where zip files will be updated
        upload_url = generate_upload_url(parser, options, redact_zip_file)
    else:
        upload_url = generate_upload_url(parser, options, zip_filename)

    # Linux
    if os.name == "posix":
        path = [
            mydir,
            "/opt/couchbase/bin",
            os.environ["PATH"],
            "/bin",
            "/sbin",
            "/usr/bin",
            "/usr/sbin",
        ]
        os.environ["PATH"] = ":".join(path)

        library_path = [os.path.join(options.root, "lib")]

        current_library_path = os.environ.get("LD_LIBRARY_PATH")
        if current_library_path is not None:
            library_path.append(current_library_path)

        os.environ["LD_LIBRARY_PATH"] = ":".join(library_path)

    # Windows
    elif os.name == "nt":
        path = [mydir, os.environ["PATH"]]
        os.environ["PATH"] = ";".join(path)

    # If user asked to just upload, then upload and exit
    if options.just_upload_into is not None:
        do_upload(args[0], options.just_upload_into, options.upload_proxy)

    # Create a TaskRunner and run all of the OS tasks (collect top, netstat, etc)
    with TaskRunner(
        verbosity=options.verbosity,
        default_name="sync_gateway.log",
        tmp_dir=options.tmp_dir,
    ) as runner:
        if not options.product_only:
            for task in make_os_tasks(["sync_gateway"]):
                runner.run(task)

        # Output the Python version if verbosity was enabled
        if options.verbosity:
            log("Python version: %s" % sys.version)

        # Find path to sg binary
        sg_binary_path = discover_sg_binary_path(options, sg_url, auth_headers)

        # Run SG specific tasks
        for task in make_sg_tasks(
            sg_url=sg_url,
            auth_headers=auth_headers,
            sync_gateway_config_path_option=options.sync_gateway_config,
            sync_gateway_executable_path=options.sync_gateway_executable,
            should_redact=should_redact,
        ):
            runner.run(task)

        if (
            sg_binary_path is not None
            and sg_binary_path != ""
            and os.path.exists(sg_binary_path)
        ):
            runner.collect_file(sg_binary_path)
        else:
            print(
                "WARNING: unable to find Sync Gateway executable, omitting from result.  Go pprofs will not be accurate."
            )

        runner.run(get_sgcollect_info_options_task(options, args))

        runner.close_all_files()

        # Build redacted zip file
        if should_redact:
            log("Redacting log files to level: %s" % options.redact_level)
            runner.redact_and_zip(
                redact_zip_file, "sgcollect_info", options.salt_value, platform.node()
            )

        try:
            # Build the actual zip file
            runner.zip(zip_filename, "sgcollect_info", platform.node())

            if options.redact_level != "none":
                print("Zipfile built: {0}".format(redact_zip_file))

            print("Zipfile built: {0}".format(zip_filename))

            if not upload_url:
                sys.exit(0)
            # Upload the zip to the URL to S3 if required
            if should_redact:
                exit_code = do_upload(redact_zip_file, upload_url, options.upload_proxy)
            else:
                exit_code = do_upload(zip_filename, upload_url, options.upload_proxy)
            sys.exit(exit_code)
        finally:
            if not options.keep_zip and upload_url:
                delete_zip(zip_filename)
                delete_zip(redact_zip_file)


def ud(value, should_redact=True):
    if not should_redact:
        return value
    return "<ud>{0}</ud>".format(value)


def get_sgcollect_info_options_task(
    options: optparse.Values, args: List[str]
) -> AllOsTask:
    """
    Create a task that returns all the options used to run sgcollect_info.
    """
    should_redact = should_redact_from_options(options)
    return AllOsTask(
        "Echo sgcollect_info cmd line args",
        "echo options: {0} args: {1}".format(
            {
                k: ud(v, should_redact)
                for k, v in list(options.__dict__.items())
                if k != "sync_gateway_password"
            },
            args,
        ),
        log_file="sgcollect_info_options.log",
    )


def should_redact_from_options(options: optparse.Values) -> bool:
    """
    Returns True if the redaction level is set to 'partial' or 'full'.
    """
    return options.redact_level != "none"


def get_sg_url(options: optparse.Values, auth_headers: dict[str, str]) -> str:
    """
    Gets the Sync Gateway URL for the admin port. Returns the first valid URL it can connect to, or https://127.0.0.1:4985 if it can not find one.

    1. --sync-gateway-url option, if provided.
        a. If the URL contains "://", it is used as is.
        b. If the URL does not contain "://", it will try to connect to http:// and https:// versions of the URL.
    2. http://127.0.0.1:4985
    3. https://127.0.0.1:4985
    """
    possible_urls: List[str] = []
    if options.sync_gateway_url:
        if "://" in options.sync_gateway_url:
            possible_urls.append(options.sync_gateway_url)
        else:
            possible_urls.extend(
                [
                    "http://" + options.sync_gateway_url,
                    "https://" + options.sync_gateway_url,
                ]
            )
    possible_urls.extend(["http://127.0.0.1:4985"])
    for url in possible_urls:
        if can_connect_to_sg_url(url, auth_headers):
            return url
    # Default URL if none of the above worked. The more correct way would be to return None if this doesn't work and do
    # not try to create any tasks that require connecting to admin API, but the subsequent code expects a URL to be
    # returned.
    return "https://127.0.0.1:4985"


def can_connect_to_sg_url(sg_url: str, auth_headers: dict[str, str]) -> bool:
    """
    Return true if can connect to the Sync Gateway URL with the provided username and password.
    """
    try:
        response = urlopen(url=sg_url, auth_headers=auth_headers)
        json.load(response)
    except Exception as e:
        print(f"Failed to communicate with: {sg_url} {e}")
        return False
    return True


def get_auth_headers(username: str) -> dict[str, str]:
    """
    Return the headers for authentication.
    """
    session_token_name = "SGCOLLECT_TOKEN"
    session_token = os.environ.get(session_token_name)
    if session_token:
        return {"Authorization": f"SGCollect {session_token}"}
    if not username:
        username = os.environ.get(SG_USERNAME_ENV, "")
    if not username:
        return {}

    password = os.environ.get(SG_PASSWORD_ENV)
    if not password:
        password = getpass.getpass(
            prompt=f"Sync Gateway Admin API password for user '{username}': "
        )
    return {"Authorization": get_basic_authorization_header(username, password)}


def get_basic_authorization_header(username: str, password: str) -> str:
    """
    Return the value for the Authorization header for basic auth.
    """
    base64string = base64.b64encode((f"{username}:{password}").encode("utf-8")).decode(
        "utf-8"
    )
    return f"Basic {base64string}"
