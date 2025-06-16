#!/usr/bin/env python

"""
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
"""

# -*- python -*-
import atexit
import gzip
import hashlib
import optparse
import os
import pathlib
import re
import shutil
import sys
import tempfile
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from copy import copy
from typing import Callable, List, Optional, Union


class LogRedactor:
    def __init__(self, salt: str, tmpdir: Union[str, pathlib.Path]):
        self.target_dir = os.path.join(tmpdir, "redacted")
        os.makedirs(self.target_dir)

        self.couchbase_log = CouchbaseLogProcessor(salt)
        self.regular_log = RegularLogProcessor(salt)

    def _process_file(self, ifile, ofile, processor):
        try:
            with get_open_fn(ifile)(
                ifile,
                "rb",
            ) as inp:
                with get_open_fn(ofile)(
                    ofile,
                    "wb+",
                ) as out:
                    # Write redaction header
                    out.write(self.couchbase_log.do(b"RedactLevel"))
                    for line in inp:
                        out.write(processor.do(line))
        except IOError as e:
            log("I/O error(%s): %s" % (e.errno, e.strerror))

    def redact_file(self, name, ifile):
        _, filename = os.path.split(name)
        ofile = os.path.join(self.target_dir, filename)
        self._process_file(ifile, ofile, self.regular_log)
        return ofile


class CouchbaseLogProcessor:
    def __init__(self, salt: str):
        self.salt = salt.encode("ascii")

    def do(self, line: bytes) -> bytes:
        if b"RedactLevel" in line:
            # salt + salt to maintain consistency with other
            # occurrences of hashed salt in the logs.
            return b"RedactLevel:partial,HashOfSalt:%b" % generate_hash(
                self.salt + self.salt
            ).hexdigest().encode("utf-8") + os.linesep.encode("ascii")
        else:
            return line


class RegularLogProcessor:
    def __init__(self, salt: str):
        self.salt = salt.encode("ascii")
        self.rexes = [
            (re.compile(b"(<ud>)(.+?)(</ud>)"), self._redact_userdata),
            (re.compile(rb"(log-redaction-salt)(.+)"), self.redact_salt),
        ]

    def _redact_userdata(self, match: re.Match) -> bytes:
        result = match.group(1)
        h = generate_hash(self.salt + match.group(2)).hexdigest().encode("ascii")
        result += h + match.group(3)
        return result

    def redact_salt(self, match: re.Match) -> bytes:
        result = match.group(1)
        result += b" <redacted>"
        # on windows, make sure we don't lose the \r
        if match.group(0).endswith(b"\r"):
            result += b"\r"
        return result

    def do(self, line: bytes) -> bytes:
        for rex, func in self.rexes:
            line = rex.sub(func, line)
        return line


def generate_hash(val: bytes):
    return hashlib.sha1(val)


class AltExitC(object):
    def __init__(self):
        self.list = []
        self.lock = threading.Lock()
        atexit.register(self.at_exit_handler)

    def register(self, f):
        self.lock.acquire()
        self.register_and_unlock(f)

    def register_and_unlock(self, f):
        try:
            self.list.append(f)
        finally:
            self.lock.release()

    def at_exit_handler(self):
        self.lock.acquire()
        self.list.reverse()
        for f in self.list:
            try:
                f()
            except Exception:
                pass

    def exit(self, status):
        self.at_exit_handler()
        os._exit(status)


AltExit = AltExitC()


def log(message, end="\n"):
    sys.stderr.write(message + end)
    sys.stderr.flush()


class Task(object):
    privileged = False
    no_header = False
    num_samples = 1
    interval = 0

    def __init__(self, description, command, timeout=None, **kwargs):
        self.description = description
        self.command = command
        self.timeout = timeout
        self.__dict__.update(kwargs)

    def execute(self, fp):
        """Run the task"""
        import subprocess

        use_shell = not isinstance(self.command, list)
        if "literal" in self.__dict__:
            fp.write(self.literal.encode("utf-8"))
            return 0

        env = None
        if "addenv" in self.__dict__:
            env = os.environ.copy()
            env.update(self.addenv)
        try:
            p = subprocess.Popen(
                self.command,
                bufsize=-1,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                shell=use_shell,
                env=env,
            )
        except OSError as e:
            # if use_shell is False then Popen may raise exception
            # if binary is missing. In this case we mimic what
            # shell does. Namely, complaining to stderr and
            # setting non-zero status code. It's might also
            # automatically handle things like "failed to fork due
            # to some system limit".
            fp.write(f"Failed to execute {self.command}: {e}".encode("utf-8"))
            return 127
        p.stdin.close()

        from threading import Event, Timer

        timer = None
        timer_fired = Event()

        if self.timeout is not None and hasattr(p, "kill"):

            def on_timeout():
                p.kill()
                timer_fired.set()

            timer = Timer(self.timeout, on_timeout)
            timer.start()

        try:
            while True:
                data = p.stdout.read(64 * 1024)
                if not data:
                    break

                fp.write(data)
        finally:
            if timer is not None:
                timer.cancel()
                timer.join()

                # there's a tiny chance that command succeeds just before
                # timer is fired; that would result in a spurious timeout
                # message
                if timer_fired.isSet():
                    fp.write(
                        f"`{self.command}` timed out after {self.timeout} seconds".encode(
                            "utf-8"
                        )
                    )

        return p.wait()

    def will_run(self):
        """Determine if this task will run on this platform."""
        return sys.platform.startswith(tuple(self.platforms))


class PythonTask(object):
    """
    A task that takes a python function as an argument rather than an OS command.
    These will run on any platform.
    """

    privileged = False
    no_header = False
    num_samples = 1
    interval = 0

    def __init__(self, description, callable, timeout=None, **kwargs):
        self.description = description
        self.callable = callable
        self.command = "pythontask"
        self.timeout = timeout
        self.log_exception = (
            False  # default to false, may be overridden by val in **kwargs
        )
        self.__dict__.update(kwargs)

    def execute(self, fp):
        """Run the task"""
        print("log_file: {0}. ".format(self.log_file))
        try:
            result = self.callable()
            try:
                fp.write(result.encode())
            except (UnicodeEncodeError, AttributeError):
                fp.write(result)
            return 0
        except Exception as e:
            if self.log_exception:
                print("Exception executing python task: {0}".format(e))
            return 1

    def will_run(self):
        """Determine if this task will run on this platform."""
        return True


class TaskRunner(object):
    def __init__(self, verbosity=0, default_name="couchbase.log", tmp_dir=None):
        self.files = {}
        self.tasks = {}
        self.verbosity = verbosity
        self.start_time = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
        self.default_name = default_name

        if not tmp_dir:
            tmp_dir = None
        else:
            tmp_dir = os.path.abspath(os.path.expanduser(tmp_dir))

        try:
            self.tmpdir = tempfile.mkdtemp(dir=tmp_dir)
        except OSError as e:
            print("Could not use temporary dir {0}: {1}".format(tmp_dir, e))
            sys.exit(1)

        # If a dir wasn't passed by --tmp-dir, check if the env var was set and if we were able to use it
        if (
            not tmp_dir
            and os.getenv("TMPDIR")
            and os.path.split(self.tmpdir)[0] != os.getenv("TMPDIR")
        ):
            log("Could not use TMPDIR {0}".format(os.getenv("TMPDIR")))
            log("Using temporary dir {0}".format(os.path.split(self.tmpdir)[0]))

        AltExit.register(self.finalize)

    def finalize(self):
        try:
            for fp in self.files.items():
                fp.close()
        except Exception:
            pass

        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def collect_file(self, filename):
        """Add a file to the list of files collected. Used to capture the exact
        file (including timestamps) from the Couchbase instance.
        filename - Absolute path to file to collect.
        """
        if filename not in self.files:
            self.files[filename] = open(filename, "r")
        else:
            log("Unable to collect file '{0}' - already collected.".format(filename))

    def get_file(self, filename):
        if filename in self.files:
            fp = self.files[filename]
        else:
            fp = open(os.path.join(self.tmpdir, filename), "wb+")
            self.files[filename] = fp

        return fp

    def header(self, fp, title, subtitle):
        separator = "=" * 78
        message = f"{separator}\n{title}\n{subtitle}\n{separator}\n"
        fp.write(message.encode())

    def log_result(self, result):
        if result == 0:
            log("OK")
        else:
            log("Exit code %d" % result)

    def run(self, task):
        """Run a task with a file descriptor corresponding to its log file"""
        command_to_print = getattr(task, "command_to_print", task.command)
        if task.will_run():
            log("%s (%s) - " % (task.description, command_to_print), end="")
            if task.privileged and os.getuid() != 0:
                log("skipped (needs root privs)")
                return

            if hasattr(task, "log_file"):
                filename = task.log_file
            else:
                filename = self.default_name

            fp = self.get_file(filename)
            if not task.no_header:
                self.header(fp, task.description, command_to_print)

            for i in range(task.num_samples):
                if i > 0:
                    log(
                        "Taking sample %d after %f seconds - " % (i + 1, task.interval),
                        end="",
                    )
                    time.sleep(task.interval)
                result = task.execute(fp)
                self.log_result(result)
            fp.flush()

        elif self.verbosity >= 2:
            log(
                'Skipping "%s" (%s): not for platform %s'
                % (task.description, command_to_print, sys.platform)
            )

    def redact_and_zip(self, filename: str, log_type: str, salt: str, node: str):
        """
        Redacts all files defined by writing a copy into a temporary directory. Zips up the directory as filename.

        :param filename: The name of the zip file to be created
        :param log_type: Prefix for the name of the zipfile
        :param salt: The salt to be used for redaction
        :param node: Hostname
        """
        files = []
        redactor = LogRedactor(salt, self.tmpdir)

        for name, fp in self.files.items():
            if redactable_file(name):
                files.append(redactor.redact_file(name, fp.name))
            else:
                files.append(fp.name)

        prefix = f"{log_type}_{node}_{self.start_time}"
        self.__make_zip(prefix, filename, files)

    def zip(self, filename, log_type, node):
        files = [file.name for name, file in self.files.items()]
        prefix = f"{log_type}_{node}_{self.start_time}"
        self.__make_zip(prefix, filename, files)

    def close_all_files(self):
        for name, fp in self.files.items():
            fp.close()

    @staticmethod
    def __make_zip(prefix, filename, files):
        """Write all our logs to a zipfile"""

        from zipfile import ZIP_DEFLATED, ZipFile

        zf = ZipFile(filename, mode="w", compression=ZIP_DEFLATED)
        try:
            for name in files:
                zf.write(name, f"{prefix}/{os.path.basename(name)}")
        finally:
            zf.close()


class LinuxTask(Task):
    platforms = ["linux"]


class WindowsTask(Task):
    platforms = ["win32", "cygwin"]


class MacOSXTask(Task):
    platforms = ["darwin"]


class UnixTask(LinuxTask, MacOSXTask):
    platforms = LinuxTask.platforms + MacOSXTask.platforms


class AllOsTask(UnixTask, WindowsTask):
    platforms = UnixTask.platforms + WindowsTask.platforms


def make_curl_task(
    name,
    url,
    auth_headers: dict[str, str],
    content_postprocessors: Optional[List[Callable]] = None,
    timeout=60,
    log_file="python_curl.log",
    **kwargs,
) -> PythonTask:
    """
    NOTE: this used to use curl but was later reworked to use pure python
    in order to be more cross platform, since Windows doesn't ship with curl

    The content_postprocessors is a list of functions that:

        - Are given a string as their only parameter
        - Return a string as their only return value

    For example:

    def reverser(s):
        return s[::-1]  # reverse string

    They are run in order.  This allows for stripping out passwords and other
    sensitive info

    """

    def python_curl_task() -> str:
        r = urllib.request.Request(url=url, headers=auth_headers, method="GET")
        try:
            response_file_handle = urllib.request.urlopen(r, timeout=timeout)
        except urllib.error.URLError as e:
            print("WARNING: Error connecting to url {0}: {1}".format(url, e))

        response_string = response_file_handle.read()
        if content_postprocessors:
            for content_postprocessor in content_postprocessors:
                response_string = content_postprocessor(response_string)
        return response_string

    return PythonTask(
        description=name, callable=python_curl_task, log_file=log_file, **kwargs
    )


def add_file_task(
    sourcefile_path: Union[pathlib.Path, str],
    output_path: Union[pathlib.Path, str],
    content_postprocessors: Optional[List[Callable]] = None,
):
    """
    Adds the contents of a file to the output zip

    The content_postprocessors is a list of functions -- see make_curl_task
    """

    def python_add_file_task():
        with open(sourcefile_path, "br") as infile:
            contents = infile.read()
            if content_postprocessors:
                for content_postprocessor in content_postprocessors:
                    contents = content_postprocessor(contents)
            return contents

    task = PythonTask(
        description="Contents of {0}".format(sourcefile_path),
        callable=python_add_file_task,
        log_file=output_path,
        log_exception=False,
    )

    return task


def make_event_log_task():
    from datetime import datetime, timedelta

    # I found that wmic ntevent can be extremely slow; so limiting the output
    # to approximately last month
    limit = datetime.today() - timedelta(days=31)
    limit = limit.strftime("%Y%m%d000000.000000-000")

    return WindowsTask(
        "Event log",
        "wmic ntevent where "
        '"'
        "(LogFile='application' or LogFile='system') and "
        "EventType<3 and TimeGenerated>'%(limit)s'"
        '" '
        "get TimeGenerated,LogFile,SourceName,EventType,Message,InsertionStrings "
        "/FORMAT:list" % locals(),
    )


def make_event_log_task_sg_info():
    from datetime import datetime, timedelta

    # I found that wmic ntevent can be extremely slow; so limiting the output
    # to approximately last month
    limit = datetime.today() - timedelta(days=31)
    limit = limit.strftime("%Y%m%d000000.000000-000")

    return WindowsTask(
        "SG Event log",
        "wmic ntevent where "
        '"'
        "SourceName='SyncGateway' and "
        "TimeGenerated>'%(limit)s'"
        '" '
        "get TimeGenerated,LogFile,SourceName,EventType,InsertionStrings "
        "/FORMAT:list" % locals(),
    )


def make_os_tasks(processes):
    programs = " ".join(processes)

    _tasks = [
        UnixTask("uname", "uname -a"),
        UnixTask("time and TZ", "date; date -u"),
        UnixTask(
            "ntp time",
            "ntpdate -q pool.ntp.org || nc time.nist.gov 13 || netcat time.nist.gov 13",
            timeout=60,
        ),
        UnixTask("ntp peers", "ntpq -p"),
        UnixTask("raw /etc/sysconfig/clock", "cat /etc/sysconfig/clock"),
        UnixTask("raw /etc/timezone", "cat /etc/timezone"),
        WindowsTask("System information", "systeminfo"),
        WindowsTask("Computer system", "wmic computersystem"),
        WindowsTask("Computer OS", "wmic os"),
        LinuxTask("System Hardware", "lshw -json || lshw"),
        LinuxTask("Raw /proc/vmstat", "cat /proc/vmstat"),
        LinuxTask("Raw /proc/mounts", "cat /proc/mounts"),
        LinuxTask("Raw /proc/partitions", "cat /proc/partitions"),
        LinuxTask(
            "Raw /proc/diskstats",
            "cat /proc/diskstats; echo ''",
            num_samples=10,
            interval=1,
        ),
        LinuxTask("Raw /proc/interrupts", "cat /proc/interrupts"),
        LinuxTask("Swap configuration", "free -t"),
        LinuxTask("Swap configuration", "swapon -s"),
        LinuxTask("Kernel modules", "lsmod"),
        LinuxTask("Distro version", "cat /etc/redhat-release"),
        LinuxTask("Distro version", "lsb_release -a"),
        LinuxTask("Distro version", "cat /etc/SuSE-release"),
        LinuxTask("Distro version", "cat /etc/issue"),
        LinuxTask("Installed software", "rpm -qa"),
        # NOTE: AFAIK columns _was_ necessary, but it doesn't appear to be
        # required anymore. I.e. dpkg -l correctly detects stdout as not a
        # tty and stops playing smart on formatting. Lets keep it for few
        # years and then drop, however.
        LinuxTask("Installed software", "COLUMNS=300 dpkg -l"),
        LinuxTask("Extended iostat", "iostat -x -p ALL 1 10 || iostat -x 1 10"),
        LinuxTask(
            "Core dump settings",
            "find /proc/sys/kernel -type f -name '*core*' -print -exec cat '{}' ';'",
        ),
        UnixTask("sysctl settings", "sysctl -a"),
        LinuxTask(
            "Relevant lsof output",
            "echo %(programs)s | xargs -n1 pgrep | xargs -n1 -r -- lsof -n -p"
            % locals(),
        ),
        LinuxTask("LVM info", "lvdisplay"),
        LinuxTask("LVM info", "vgdisplay"),
        LinuxTask("LVM info", "pvdisplay"),
        MacOSXTask("Disk activity", "iostat 1 10"),
        MacOSXTask(
            "Process list",
            "ps -Aww -o user,pid,lwp,ppid,nlwp,pcpu,pri,nice,vsize,rss,tty,"
            "stat,wchan:12,start,bsdtime,command",
        ),
        WindowsTask("Installed software", "wmic product get name, version"),
        WindowsTask(
            "Service list",
            'wmic service where state="running" GET caption, name, state',
        ),
        WindowsTask("Process list", "wmic process"),
        WindowsTask("Process usage", "tasklist /V /fo list"),
        WindowsTask("Swap settings", "wmic pagefile"),
        WindowsTask("Disk partition", "wmic partition"),
        WindowsTask("Disk volumes", "wmic volume"),
        UnixTask("Network configuration", "ifconfig -a", interval=10, num_samples=2),
        LinuxTask(
            "Network configuration",
            "echo link addr neigh rule route netns | xargs -n1 -- sh -x -c 'ip $1 list' --",
        ),
        WindowsTask(
            "Network configuration", "ipconfig /all", interval=10, num_samples=2
        ),
        LinuxTask("Raw /proc/net/dev", "cat /proc/net/dev"),
        LinuxTask("Network link statistics", "ip -s link"),
        UnixTask("Network status", "netstat -anp || netstat -an"),
        WindowsTask("Network status", "netstat -ano"),
        AllOsTask("Network routing table", "netstat -rn"),
        LinuxTask("Network socket statistics", "ss -an"),
        LinuxTask("Extended socket statistics", "ss -an --info --processes"),
        UnixTask("Arp cache", "arp -na"),
        LinuxTask("Iptables dump", "iptables-save"),
        UnixTask("Raw /etc/hosts", "cat /etc/hosts"),
        UnixTask("Raw /etc/resolv.conf", "cat /etc/resolv.conf"),
        UnixTask("Raw /etc/nsswitch.conf", "cat /etc/nsswitch.conf"),
        WindowsTask("Arp cache", "arp -a"),
        WindowsTask("Network Interface Controller", "wmic nic"),
        WindowsTask("Network Adapter", "wmic nicconfig"),
        WindowsTask("Active network connection", "wmic netuse"),
        WindowsTask("Protocols", "wmic netprotocol"),
        WindowsTask("Hosts file", r"type %SystemRoot%\system32\drivers\etc\hosts"),
        WindowsTask("Cache memory", "wmic memcache"),
        WindowsTask("Physical memory", "wmic memphysical"),
        WindowsTask("Physical memory chip info", "wmic memorychip"),
        WindowsTask("Local storage devices", "wmic logicaldisk"),
        UnixTask("Filesystem", "df -ha"),
        UnixTask("System activity reporter", "sar 1 10"),
        UnixTask("System paging activity", "vmstat 1 10"),
        UnixTask("System uptime", "uptime"),
        UnixTask("couchbase user definition", "getent passwd couchbase"),
        UnixTask(
            "couchbase user limits", 'su couchbase -c "ulimit -a"', privileged=True
        ),
        UnixTask("sync_gateway user definition", "getent passwd sync_gateway"),
        UnixTask(
            "sync_gateway user limits",
            'su sync_gateway -c "ulimit -a"',
            privileged=True,
        ),
        UnixTask("Interrupt status", "intrstat 1 10"),
        UnixTask("Processor status", "mpstat 1 10"),
        UnixTask("System log", "cat /var/adm/messages"),
        LinuxTask("Raw /proc/uptime", "cat /proc/uptime"),
        LinuxTask(
            "Systemd journal",
            "journalctl 2>&1 | gzip -c",
            log_file="systemd_journal.gz",
            no_header=True,
        ),
        LinuxTask(
            "All logs",
            "tar cz /var/log/syslog* /var/log/dmesg /var/log/messages* /var/log/daemon* /var/log/debug* /var/log/kern.log* 2>/dev/null",
            log_file="syslog.tar.gz",
            no_header=True,
        ),
        LinuxTask(
            "Relevant proc data",
            "echo %(programs)s | "
            "xargs -n1 pgrep | xargs -n1 -- sh -c 'echo $1; cat /proc/$1/status; cat /proc/$1/limits; cat /proc/$1/smaps; cat /proc/$1/numa_maps; cat /proc/$1/task/*/sched; echo' --"
            % locals(),
        ),
        LinuxTask(
            "Processes' environment",
            "echo %(programs)s | "
            r"xargs -n1 pgrep | xargs -n1 -- sh -c 'echo $1; ( cat /proc/$1/environ | tr \\0 \\n ); echo' --"
            % locals(),
        ),
        LinuxTask("NUMA data", "numactl --hardware"),
        LinuxTask("NUMA data", "numactl --show"),
        LinuxTask("NUMA data", "cat /sys/devices/system/node/node*/numastat"),
        UnixTask("Kernel log buffer", "dmesg -H || dmesg"),
        LinuxTask(
            "Transparent Huge Pages data",
            "cat /sys/kernel/mm/transparent_hugepage/enabled",
        ),
        LinuxTask(
            "Transparent Huge Pages data",
            "cat /sys/kernel/mm/transparent_hugepage/defrag",
        ),
        LinuxTask(
            "Transparent Huge Pages data",
            "cat /sys/kernel/mm/redhat_transparent_hugepage/enabled",
        ),
        LinuxTask(
            "Transparent Huge Pages data",
            "cat /sys/kernel/mm/redhat_transparent_hugepage/defrag",
        ),
        LinuxTask("Network statistics", "netstat -s"),
        LinuxTask("Full raw netstat", "cat /proc/net/netstat"),
        LinuxTask(
            "CPU throttling info",
            "echo /sys/devices/system/cpu/cpu*/thermal_throttle/* | xargs -n1 -- sh -c 'echo $1; cat $1' --",
        ),
        make_event_log_task(),
        make_event_log_task_sg_info(),
    ]

    return _tasks


# stolen from http://rightfootin.blogspot.com/2006/09/more-on-python-flatten.html
def iter_flatten(iterable):
    it = iter(iterable)
    for e in it:
        if isinstance(e, (list, tuple)):
            for f in iter_flatten(e):
                yield f
        else:
            yield e


def flatten(iterable):
    return [e for e in iter_flatten(iterable)]


def setup_stdin_watcher():
    def _in_thread():
        sys.stdin.readline()
        AltExit.exit(2)

    th = threading.Thread(target=_in_thread)
    th.setDaemon(True)
    th.start()


def do_upload(path, url, proxy):
    """
    Uploads file path to a URL and returns exit code for the program.
    """

    with open(path, "rb") as f:
        # Get proxies from environment/system
        proxy_handler = urllib.request.ProxyHandler(urllib.request.getproxies())
        if proxy != "":
            # unless a proxy is explicitly passed, then use that instead
            proxy_handler = urllib.request.ProxyHandler({"https": proxy, "http": proxy})

        opener = urllib.request.build_opener(proxy_handler)
        request = urllib.request.Request(url, data=f, method="PUT")
        request.add_header(str("Content-Type"), str("application/zip"))
        request.add_header("Content-Length", os.fstat(f.fileno()).st_size)

        try:
            url = opener.open(request)
            if url.getcode() == 200:
                log("Done uploading")
            else:
                raise Exception(
                    "Error uploading, expected status code 200, got status code: {0}".format(
                        url.getcode()
                    )
                )
        except Exception:
            log(traceback.format_exc())
            return 1

    return 0


def parse_host(host):
    url = urllib.parse.urlsplit(host)
    if not url.scheme:
        url = urllib.parse.urlsplit("https://" + host)

    return url.scheme, url.netloc, url.path


def generate_upload_url(parser, options, zip_filename):
    upload_url = None
    if options.upload_host:
        if not options.upload_customer:
            parser.error("Need --customer when --upload-host is given")

        scheme, netloc, path = parse_host(options.upload_host)

        customer = urllib.parse.quote(options.upload_customer)
        fname = urllib.parse.quote(os.path.basename(zip_filename))
        if options.upload_ticket:
            full_path = "%s/%s/%d/%s" % (path, customer, options.upload_ticket, fname)
        else:
            full_path = "%s/%s/%s" % (path, customer, fname)

        upload_url = urllib.parse.urlunsplit((scheme, netloc, full_path, "", ""))
        log("Will upload collected .zip file into %s" % upload_url)
    return upload_url


def check_ticket(option, opt, value):
    if re.match(r"^\d{1,7}$", value):
        return int(value)
    else:
        raise optparse.OptionValueError(
            "option %s: invalid ticket number: %r" % (opt, value)
        )


class CbcollectInfoOptions(optparse.Option):
    TYPES = optparse.Option.TYPES + ("ticket",)
    TYPE_CHECKER = copy(optparse.Option.TYPE_CHECKER)
    TYPE_CHECKER["ticket"] = check_ticket


def redactable_file(filename: Union[pathlib.Path, str]) -> bool:
    """
    Return True if the file should be redacted, otherwise False.
    """
    filename = pathlib.Path(filename)
    if filename.name.startswith(("pprof", "expvars.json")):
        return False
    return filename.stem != "sync_gateway"


def get_open_fn(path: Union[pathlib.Path, str]) -> Callable:
    """
    Return open function for path. gzip.open if suffixed with .gz, else open.
    """
    path = pathlib.Path(path)
    if path.suffix == ".gz":
        return gzip.open
    return open
