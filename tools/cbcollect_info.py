#!/usr/bin/env python
# -*- python -*-
#
# @author Couchbase <info@couchbase.com>
# @copyright 2011-2018 Couchbase, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os
import sys
import tempfile
import time
import subprocess
import string
import re
import platform
import glob
import socket
import threading
import optparse
import atexit
import signal
import urllib
import shutil
import urlparse
import errno
import hashlib
import uuid
from datetime import datetime, timedelta, tzinfo
from StringIO import StringIO

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
            except:
                pass

    def exit(self, status):
        self.at_exit_handler()
        os._exit(status)

AltExit = AltExitC()

USAGE = """usage: %prog [options] output_file.zip

- Linux/Windows/OSX:
    %prog output_file.zip
    %prog -v output_file.zip"""

# adapted from pytz
class FixedOffsetTZ(tzinfo):
    def __init__(self, minutes):
        if abs(minutes) >= 1440:
            raise ValueError("absolute offset is too large", minutes)
        self._minutes = minutes
        self._offset = timedelta(minutes=minutes)

    def utcoffset(self, dt):
        return self._offset

    def dst(self, dt):
        return timedelta(0)

    def tzname(self, dt):
        return None

local_tz = FixedOffsetTZ(minutes=time.timezone / 60)
log_stream = StringIO()
local_addr = None
local_url_addr = None

def set_local_addr(ipv6):
    global local_addr
    global local_url_addr

    local_addr = "::1" if ipv6 else "127.0.0.1"
    local_url_addr = "[::1]" if ipv6 else "127.0.0.1"

log_line = None
def buffer_log_line(message, new_line):
    global log_line

    line = log_line
    if line is None:
        now = datetime.now(tz=local_tz)
        line = '[%s] ' % now.isoformat()

    line += message
    if new_line:
        log_line = None
        return line
    else:
        log_line = line
        return None

def log(message, new_line=True):
    global log_stream

    if new_line:
        message += '\n'

    bufline = buffer_log_line(message, new_line)
    if bufline is not None:
        log_stream.write(bufline)

    sys.stderr.write(message)
    sys.stderr.flush()

class AccessLogProcessor:
    def __init__(self, salt):
        self.salt = salt
        self.column_parser = re.compile(
            r'(^\S* \S* )(\S*)( \[.*\] \"\S* )(\S*)( .*$)')
        self.urls_to_redact = [['/settings/rbac/users',
                                re.compile(r'\/(?P<user>[^\/\s#&]+)([#&]|$)')],
                               ['/_cbauth/checkPermission',
                                re.compile(r'user=(?P<user>[^\s&#]+)')]]

    def _process_url(self, surl):
        for conf in self.urls_to_redact:
            prefix = conf[0]
            if surl[:len(prefix)] == prefix:
                return prefix + self._process_url_tail(conf[1],
                                                       surl[len(prefix):])
        return surl

    def _process_url_tail(self, rex, s):
        m = rex.search(s)
        if m != None:
            return s[:m.start("user")] + self._process_user(m.group("user")) + \
                s[m.end("user"):]
        else:
            return s

    def _process_user(self, user):
        if user == '-' or user[0] == '@':
            return user
        elif user[-3:] == "/UI":
            return self._hash(user[:-3]) + "/UI"
        else:
            return self._hash(user)

    def _hash(self, token):
        return hashlib.sha1(self.salt + token).hexdigest()

    def _repl_columns(self, matchobj):
        return matchobj.group(1) + \
            self._process_user(matchobj.group(2)) + \
            matchobj.group(3) + \
            self._process_url(matchobj.group(4)) + \
            matchobj.group(5)

    def do(self, line):
        return self.column_parser.sub(self._repl_columns, line)

class CouchbaseLogProcessor:
    def __init__(self, salt):
        self.salt = salt

    def do(self, line):
        if "RedactLevel" in line:
            return 'RedactLevel:partial,HashOfSalt:%s\n' \
                % hashlib.sha1(self.salt).hexdigest()
        else:
            return line

class RegularLogProcessor:
    def __init__(self, salt):
        self.salt = salt
        self.rex = re.compile('(<ud>)(.+?)(</ud>)')

    def _hash(self, match):
        h = hashlib.sha1(self.salt + match.group(2)).hexdigest()
        return match.group(1) + h + match.group(3)

    def do(self, line):
        return self.rex.sub(self._hash, line)

class LogRedactor:
    def __init__(self, salt, tmpdir, default_name):
        self.default_name = default_name
        self.target_dir = os.path.join(tmpdir, "redacted")
        os.makedirs(self.target_dir)

        self.access_log = AccessLogProcessor(salt)
        self.couchbase_log = CouchbaseLogProcessor(salt)
        self.regular_log = RegularLogProcessor(salt)

    def _process_file(self, ifile, ofile, processor):
        try:
            with open(ifile, 'r') as inp:
                with open(ofile, 'w+') as out:
                    for line in inp:
                        out.write(processor.do(line))
        except IOError as e:
            log("I/O error(%s): %s" % (e.errno, e.strerror))

    def redact_file(self, name, ifile):
        ofile = os.path.join(self.target_dir, name)
        if "http_access" in name:
            self._process_file(ifile, ofile, self.access_log)
        elif name == self.default_name:
            self._process_file(ifile, ofile, self.couchbase_log)
        else:
            self._process_file(ifile, ofile, self.regular_log)
        return ofile

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
        self._is_posix = (os.name == 'posix')

    def _platform_popen_flags(self):
        flags = {}
        if self._is_posix:
            flags['preexec_fn'] = os.setpgrp

        return flags

    def _can_kill(self, p):
        if self._is_posix:
            return True

        return hasattr(p, 'kill')

    def _kill(self, p):
        if self._is_posix:
            group_pid = os.getpgid(p.pid)
            os.killpg(group_pid, signal.SIGKILL)
        else:
            p.kill()

    def _env_flags(self):
        flags = {}
        if hasattr(self, 'addenv'):
            env = os.environ.copy()
            env.update(self.addenv)
            flags['env'] = env

        return flags

    def _cwd_flags(self):
        flags = {}
        if getattr(self, 'change_dir', False):
            cwd = self._task_runner.tmpdir
            if isinstance(self.change_dir, str):
                cwd = self.change_dir

            flags['cwd'] = cwd

        return flags

    def _extra_flags(self):
        flags = self._env_flags()
        flags.update(self._platform_popen_flags())
        flags.update(self._cwd_flags())

        return flags

    def set_task_runner(self, runner):
        self._task_runner = runner

    def execute(self, fp):
        """Run the task"""
        use_shell = not isinstance(self.command, list)
        extra_flags = self._extra_flags()
        try:
            p = subprocess.Popen(self.command, bufsize=-1,
                                 stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT,
                                 shell=use_shell,
                                 **extra_flags)
            if hasattr(self, 'to_stdin'):
                p.stdin.write(self.to_stdin)

            p.stdin.close()

        except OSError, e:
            # if use_shell is False then Popen may raise exception
            # if binary is missing. In this case we mimic what
            # shell does. Namely, complaining to stderr and
            # setting non-zero status code. It's might also
            # automatically handle things like "failed to fork due
            # to some system limit".
            print >> fp, "Failed to execute %s: %s" % (self.command, e)
            return 127

        except IOError, e:
            if e.errno == errno.EPIPE:
                print >> fp, "Ignoring broken pipe on stdin for %s" % self.command
            else:
                raise

        from threading import Timer, Event

        timer = None
        timer_fired = Event()

        if self.timeout is not None and self._can_kill(p):
            def on_timeout():
                try:
                    self._kill(p)
                except:
                    # the process might have died already
                    pass

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
                    print >> fp, "`%s` timed out after %s seconds" % (self.command, self.timeout)
                    log("[Command timed out after %s seconds] - " % (self.timeout), new_line=False)

        return p.wait()

    def will_run(self):
        """Determine if this task will run on this platform."""
        return sys.platform in self.platforms


class TaskRunner(object):
    default_name = "couchbase.log"

    def __init__(self, verbosity=0, task_regexp='', tmp_dir=None,
                 salt_value=""):
        self.files = {}
        self.verbosity = verbosity
        self.start_time = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
        self.salt_value = salt_value

        # Depending on platform, mkdtemp() may act unpredictably if passed an empty string.
        if not tmp_dir:
            tmp_dir = None
        else:
            tmp_dir = os.path.abspath(os.path.expanduser(tmp_dir))

        try:
            self.tmpdir = tempfile.mkdtemp(dir=tmp_dir)
        except OSError as e:
           print "Could not use temporary dir {0}: {1}".format(tmp_dir, e)
           sys.exit(1)

        # If a dir wasn't passed by --tmp-dir, check if the env var was set and if we were able to use it
        if not tmp_dir and os.getenv("TMPDIR") and os.path.split(self.tmpdir)[0] != os.getenv("TMPDIR"):
                log("Could not use TMPDIR {0}".format(os.getenv("TMPDIR")))
        log("Using temporary dir {0}".format(os.path.split(self.tmpdir)[0]))

        self.task_regexp = re.compile(task_regexp)

        AltExit.register(self.finalize)

    def finalize(self):
        try:
            for fp in self.files.iteritems():
                fp.close()
        except:
            pass

        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def collect_file(self, filename):
        """Add a file to the list of files collected. Used to capture the exact
        file (including timestamps) from the Couchbase instance.

        filename - Absolute path to file to collect.
        """
        if not filename in self.files:
            try:
                self.files[filename] = open(filename, 'r')
            except IOError, e:
                log("Failed to collect file '%s': %s" % (filename, str(e)))
        else:
            log("Unable to collect file '%s' - already collected." % filename)

    def get_file(self, filename):
        if filename in self.files:
            fp = self.files[filename]
        else:
            fp = open(os.path.join(self.tmpdir, filename), 'w+')
            self.files[filename] = fp

        return fp

    def header(self, fp, title, subtitle):
        separator = '=' * 78
        print >> fp, separator
        print >> fp, title
        print >> fp, subtitle
        print >> fp, separator
        fp.flush()

    def log_result(self, result):
        if result == 0:
            log("OK")
        else:
            log("Exit code %d" % result)

    def run_tasks(self, tasks):
        for task in tasks:
            self.run(task)

    def run(self, task):
        if self.task_regexp.match(task.description) is None:
                log("Skipping task %s because "
                    "it doesn't match '%s'" % (task.description,
                                               self.task_regexp.pattern))
        else:
            self._run(task)

    def _run(self, task):
        """Run a task with a file descriptor corresponding to its log file"""
        if task.will_run():
            log("%s (%s) - " % (task.description, task.command), new_line=False)
            if task.privileged and os.getuid() != 0:
                log("skipped (needs root privs)")
                return

            task.set_task_runner(self)

            filename = getattr(task, 'log_file', self.default_name)
            fp = self.get_file(filename)
            if not task.no_header:
                self.header(fp, task.description, task.command)

            for i in xrange(task.num_samples):
                if i > 0:
                    log("Taking sample %d after %f seconds - " % (i+1, task.interval), new_line=False)
                    time.sleep(task.interval)
                result = task.execute(fp)
                self.log_result(result)

            for artifact in getattr(task, 'artifacts', []):
                path = artifact
                if not os.path.isabs(path):
                    # we assume that "relative" artifacts are produced in the
                    # self.tmpdir
                    path = os.path.join(self.tmpdir, path)

                self.collect_file(path)

            fp.flush()

        elif self.verbosity >= 2:
            log('Skipping "%s" (%s): not for platform %s' % (task.description, task.command, sys.platform))

    def literal(self, description, value, **kwargs):
        self.run(LiteralTask(description, value, **kwargs))


    def redact_and_zip(self, filename, node):
        files = []
        redactor = LogRedactor(self.salt_value, self.tmpdir, self.default_name)

        for name, fp in self.files.iteritems():
            if "users.dets" in name:
                continue
            files.append(redactor.redact_file(name, fp.name))

        prefix = "cbcollect_info_%s_%s" % (node, self.start_time)
        self._zip_helper(prefix, filename, files)

    def close_all_files(self):
        for name, fp in self.files.iteritems():
            fp.close()

    def zip(self, filename, node):
        prefix = "cbcollect_info_%s_%s" % (node, self.start_time)

        files = []
        for name, fp in self.files.iteritems():
            files.append(fp.name)
        self._zip_helper(prefix, filename, files)

    def _zip_helper(self, prefix, filename, files):
        """Write all our logs to a zipfile"""
        exe = exec_name("gozip")

        fallback = False

        try:
            p = subprocess.Popen([exe, "-strip-path", "-prefix", prefix, filename] + files,
                                 stderr=subprocess.STDOUT,
                                 stdin=subprocess.PIPE)
            p.stdin.close()
            status = p.wait()

            if status != 0:
                log("gozip terminated with non-zero exit code (%d)" % status)
        except OSError, e:
            log("Exception during compression: %s" % e)
            fallback = True

        if fallback:
            log("IMPORTANT:")
            log("  Compression using gozip failed.")
            log("  Falling back to python implementation.")
            log("  Please let us know about this and provide console output.")

            self._zip_fallback(filename, prefix, files)

    def _zip_fallback(self, filename, prefix, files):
        from zipfile import ZipFile, ZIP_DEFLATED
        zf = ZipFile(filename, mode='w', compression=ZIP_DEFLATED)
        try:
            for name in files:
                zf.write(name,
                         "%s/%s" % (prefix, os.path.basename(name)))
        finally:
            zf.close()

class SolarisTask(Task):
    platforms = ['sunos5', 'solaris']


class LinuxTask(Task):
    platforms = ['linux2']


class WindowsTask(Task):
    platforms = ['win32', 'cygwin']


class MacOSXTask(Task):
    platforms = ['darwin']


class UnixTask(SolarisTask, LinuxTask, MacOSXTask):
    platforms = SolarisTask.platforms + LinuxTask.platforms + MacOSXTask.platforms


class AllOsTask(UnixTask, WindowsTask):
    platforms = UnixTask.platforms + WindowsTask.platforms

class LiteralTask(AllOsTask):
    def __init__(self, description, literal, **kwargs):
        self.description = description
        self.command = ''
        self.literal = literal
        self.__dict__.update(kwargs)

    def execute(self, fp):
        print >> fp, self.literal
        return 0

class CollectFile(AllOsTask):
    def __init__(self, description, file_path, **kwargs):
        self.description = description
        self.command = ''
        self.file_path = file_path
        self.__dict__.update(kwargs)

    def execute(self, fp):
        self._task_runner.collect_file(self.file_path)
        print >> fp, "Collected file %s" % self.file_path
        return 0

def make_curl_task(name, user, password, url,
                   timeout=60, log_file="couchbase.log", base_task=AllOsTask,
                   **kwargs):
    return base_task(name, ["curl", "-sS", "--proxy", "", "-K-", url],
                     timeout=timeout,
                     log_file=log_file,
                     to_stdin="--user %s:%s" % (user, password),
                     **kwargs)

def make_cbstats_task(kind, memcached_pass, guts):
    port = read_guts(guts, "memcached_port")
    user = read_guts(guts, "memcached_admin")
    return AllOsTask("memcached stats %s" % kind,
                     flatten(["cbstats", "-a", "%s:%s" % (local_url_addr, port), kind, "-u", user]),
                     log_file="stats.log",
                     timeout=60,
                     addenv=[("CB_PASSWORD", memcached_pass)])

def get_local_token(guts, port):
    path = read_guts(guts, "localtoken_path")
    token = ""
    try:
        with open(path, 'r') as f:
            token = f.read().rstrip('\n')
    except IOError as e:
        log("I/O error(%s): %s" % (e.errno, e.strerror))
    return token

def get_diag_password(guts):
    port = read_guts(guts, "rest_port")
    pwd = get_local_token(guts, port)
    url = "http://%s:%s/diag/password" % (local_url_addr, port)
    command = ["curl", "-sS", "--proxy", "", "-u", "@localtoken:%s" % pwd, url]

    task = AllOsTask("get diag password", command, timeout=60)
    output = StringIO()
    if task.execute(output) == 0:
        return output.getvalue()
    else:
        log(output.getvalue())
        return ""

def make_query_task(statement, user, password, port):
    url = "http://%s:%s/query/service?statement=%s" % (local_url_addr, port, urllib.quote(statement))

    return make_curl_task(name="Result of query statement \'%s\'" % statement,
                          user=user, password=password, url=url)

def make_index_task(name, api, passwd, index_port, logfile="couchbase.log"):
    index_url = 'http://%s:%s/%s' % (local_url_addr, index_port, api)

    return make_curl_task(name, "@", passwd, index_url, log_file=logfile)

def make_redaction_task():
    return LiteralTask("Log Redaction", "RedactLevel:none")

def basedir():
    mydir = os.path.dirname(sys.argv[0])
    if mydir == "":
        mydir = "."
    return mydir

def make_event_log_task():
    from datetime import datetime, timedelta

    # I found that wmic ntevent can be extremely slow; so limiting the output
    # to approximately last month
    limit = datetime.today() - timedelta(days=31)
    limit = limit.strftime('%Y%m%d000000.000000-000')

    return WindowsTask("Event log",
                       "wmic ntevent where "
                       "\""
                       "(LogFile='application' or LogFile='system') and "
                       "EventType<3 and TimeGenerated>'%(limit)s'"
                       "\" "
                       "get TimeGenerated,LogFile,SourceName,EventType,Message "
                       "/FORMAT:list" % locals())


def make_os_tasks():
    programs = " ".join(["moxi", "memcached", "beam.smp",
                         "couch_compact", "godu", "sigar_port",
                         "cbq-engine", "indexer", "projector", "goxdcr",
                         "cbft", "eventing-producer"])

    _tasks = [
        UnixTask("uname", "uname -a"),
        UnixTask("time and TZ", "date; date -u"),
        UnixTask("ntp time",
                 "ntpdate -q pool.ntp.org || "
                 "nc time.nist.gov 13 || "
                 "netcat time.nist.gov 13", timeout=60),
        UnixTask("ntp peers", "ntpq -p"),
        UnixTask("raw /etc/sysconfig/clock", "cat /etc/sysconfig/clock"),
        UnixTask("raw /etc/timezone", "cat /etc/timezone"),
        WindowsTask("System information", "systeminfo"),
        WindowsTask("Computer system", "wmic computersystem"),
        WindowsTask("Computer OS", "wmic os"),
        LinuxTask("System Hardware", "lshw -json || lshw"),
        SolarisTask("Process list snapshot", "prstat -a -c -n 100 -t -v -L 1 10"),
        SolarisTask("Process list", "ps -ef"),
        SolarisTask("Service configuration", "svcs -a"),
        SolarisTask("Swap configuration", "swap -l"),
        SolarisTask("Disk activity", "zpool iostat 1 10"),
        SolarisTask("Disk activity", "iostat -E 1 10"),
        LinuxTask("Process list snapshot", "export TERM=''; top -Hb -n1 || top -H n1"),
        LinuxTask("Process list", "ps -AwwL -o user,pid,lwp,ppid,nlwp,pcpu,maj_flt,min_flt,pri,nice,vsize,rss,tty,stat,wchan:12,start,bsdtime,command"),
        LinuxTask("Raw /proc/vmstat", "cat /proc/vmstat"),
        LinuxTask("Raw /proc/mounts", "cat /proc/mounts"),
        LinuxTask("Raw /proc/partitions", "cat /proc/partitions"),
        LinuxTask("Raw /proc/diskstats", "cat /proc/diskstats; echo ''", num_samples=10, interval=1),
        LinuxTask("Raw /proc/interrupts", "cat /proc/interrupts"),
        LinuxTask("Swap configuration", "free -t"),
        LinuxTask("Swap configuration", "swapon -s"),
        LinuxTask("Kernel modules", "lsmod"),
        LinuxTask("Distro version", "cat /etc/redhat-release"),
        LinuxTask("Distro version", "lsb_release -a"),
        LinuxTask("Distro version", "cat /etc/SuSE-release"),
        LinuxTask("Distro version", "cat /etc/issue"),
        LinuxTask("Installed software", "rpm -qa"),
        LinuxTask("Hot fix list", "rpm -V couchbase-server"),
        # NOTE: AFAIK columns _was_ necessary, but it doesn't appear to be
        # required anymore. I.e. dpkg -l correctly detects stdout as not a
        # tty and stops playing smart on formatting. Lets keep it for few
        # years and then drop, however.
        LinuxTask("Installed software", "COLUMNS=300 dpkg -l"),
        # NOTE: -V is supported only from dpkg v1.17.2 onwards.
        LinuxTask("Hot fix list", "COLUMNS=300 dpkg -V couchbase-server"),
        LinuxTask("Extended iostat", "iostat -x -p ALL 1 10 || iostat -x 1 10"),
        LinuxTask("Core dump settings", "find /proc/sys/kernel -type f -name '*core*' -print -exec cat '{}' ';'"),
        UnixTask("sysctl settings", "sysctl -a"),
        LinuxTask("Relevant lsof output",
                  "echo %(programs)s | xargs -n1 pgrep | xargs -n1 -r -- lsof -n -p" % locals()),
        LinuxTask("LVM info", "lvdisplay"),
        LinuxTask("LVM info", "vgdisplay"),
        LinuxTask("LVM info", "pvdisplay"),
        LinuxTask("Block device queue settings",
                  "find /sys/block/*/queue -type f | xargs grep -vH xxxx | sort"),
        MacOSXTask("Process list snapshot", "top -l 1"),
        MacOSXTask("Disk activity", "iostat 1 10"),
        MacOSXTask("Process list",
                   "ps -Aww -o user,pid,lwp,ppid,nlwp,pcpu,pri,nice,vsize,rss,tty,"
                   "stat,wchan:12,start,bsdtime,command"),
        WindowsTask("Installed software", "wmic product get name, version"),
        WindowsTask("Service list", "wmic service where state=\"running\" GET caption, name, state"),
        WindowsTask("Process list", "wmic process"),
        WindowsTask("Process usage", "tasklist /V /fo list"),
        WindowsTask("Swap settings", "wmic pagefile"),
        WindowsTask("Disk partition", "wmic partition"),
        WindowsTask("Disk volumes", "wmic volume"),
        UnixTask("Network configuration", "ifconfig -a", interval=10,
                 num_samples=2),
        LinuxTask("Network configuration", "echo link addr neigh rule route netns | xargs -n1 -- sh -x -c 'ip $1 list' --"),
        WindowsTask("Network configuration", "ipconfig /all", interval=10,
                    num_samples=2),
        LinuxTask("Raw /proc/net/dev", "cat /proc/net/dev"),
        LinuxTask("Network link statistics", "ip -s link"),
        UnixTask("Network status", "netstat -anp || netstat -an"),
        WindowsTask("Network status", "netstat -anotb"),
        AllOsTask("Network routing table", "netstat -rn"),
        LinuxTask("Network socket statistics", "ss -an"),
        LinuxTask("Extended socket statistics", "ss -an --info --processes", timeout=300),
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
        WindowsTask("Hosts file", "type %SystemRoot%\system32\drivers\etc\hosts"),
        WindowsTask("Cache memory", "wmic memcache"),
        WindowsTask("Physical memory", "wmic memphysical"),
        WindowsTask("Physical memory chip info", "wmic memorychip"),
        WindowsTask("Local storage devices", "wmic logicaldisk"),
        UnixTask("Filesystem", "df -ha"),
        UnixTask("System activity reporter", "sar 1 10"),
        UnixTask("System paging activity", "vmstat 1 10"),
        UnixTask("System uptime", "uptime"),
        UnixTask("Last logins of users and ttys", "last -x || last"),
        UnixTask("couchbase user definition", "getent passwd couchbase"),
        UnixTask("couchbase user limits", "su couchbase -s /bin/sh -c \"ulimit -a\"",
                 privileged=True),
        UnixTask("Interrupt status", "intrstat 1 10"),
        UnixTask("Processor status", "mpstat 1 10"),
        UnixTask("System log", "cat /var/adm/messages"),
        LinuxTask("Raw /proc/uptime", "cat /proc/uptime"),
        LinuxTask("Systemd journal",
                  "journalctl | gzip -c > systemd_journal.gz",
                  change_dir=True, artifacts=['systemd_journal.gz']),
        LinuxTask("All logs", "tar cz /var/log/syslog* /var/log/dmesg /var/log/messages* /var/log/daemon* /var/log/debug* /var/log/kern.log* 2>/dev/null",
                  log_file="syslog.tar.gz", no_header=True),
        LinuxTask("Relevant proc data", "echo %(programs)s | "
                  "xargs -n1 pgrep | xargs -n1 -- sh -c 'echo $1; cat /proc/$1/status; cat /proc/$1/limits; cat /proc/$1/smaps; cat /proc/$1/numa_maps; cat /proc/$1/task/*/sched; echo' --" % locals()),
        LinuxTask("Processes' environment", "echo %(programs)s | "
                  r"xargs -n1 pgrep | xargs -n1 -- sh -c 'echo $1; ( cat /proc/$1/environ | tr \\0 \\n | egrep -v ^CB_MASTER_PASSWORD=\|^CBAUTH_REVRPC_URL=); echo' --" % locals()),
        LinuxTask("Processes' stack",
                  "for program in %(programs)s; do for thread in $(pgrep --lightweight $program); do echo $program/$thread:; cat /proc/$thread/stack; echo; done; done" % locals()),
        LinuxTask("NUMA data", "numactl --hardware"),
        LinuxTask("NUMA data", "numactl --show"),
        LinuxTask("NUMA data", "cat /sys/devices/system/node/node*/numastat"),
        UnixTask("Kernel log buffer", "dmesg -T || dmesg -H || dmesg"),
        LinuxTask("Transparent Huge Pages data", "cat /sys/kernel/mm/transparent_hugepage/enabled"),
        LinuxTask("Transparent Huge Pages data", "cat /sys/kernel/mm/transparent_hugepage/defrag"),
        LinuxTask("Transparent Huge Pages data", "cat /sys/kernel/mm/redhat_transparent_hugepage/enabled"),
        LinuxTask("Transparent Huge Pages data", "cat /sys/kernel/mm/redhat_transparent_hugepage/defrag"),
        LinuxTask("Network statistics", "netstat -s"),
        LinuxTask("Full raw netstat", "cat /proc/net/netstat"),
        LinuxTask("CPU throttling info", "echo /sys/devices/system/cpu/cpu*/thermal_throttle/* | xargs -n1 -- sh -c 'echo $1; cat $1' --"),
        LinuxTask("Raw PID 1 scheduler /proc/1/sched", "cat /proc/1/sched | head -n 1"),
        LinuxTask("Raw PID 1 control groups /proc/1/cgroup", "cat /proc/1/cgroup"),
        make_event_log_task(),
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

def read_guts(guts, key):
    return guts.get(key, "")

def winquote_path(s):
    return '"'+s.replace("\\\\", "\\").replace('/', "\\")+'"'

# python's split splits empty string to [''] which doesn't make any
# sense. So this function works around that.
def correct_split(string, splitchar):
    rv = string.split(splitchar)
    if rv == ['']:
        rv = []
    return rv

def make_stats_archives_task(guts, initargs_path):
    escript = exec_name("escript")
    escript_wrapper = find_script("escript-wrapper")
    dump_stats = find_script("dump-stats")
    stats_dir = read_guts(guts, "stats_dir")

    if dump_stats is None or escript_wrapper is None or not stats_dir:
        return []

    output_file = "stats_archives.json"
    return AllOsTask("stats archives",
                     [escript,
                      escript_wrapper,
                      "--initargs-path", initargs_path, "--",
                      dump_stats, stats_dir, output_file],
                     change_dir=True,
                     artifacts=[output_file])

def make_product_task(guts, initargs_path, memcached_pass, options):
    root = os.path.abspath(os.path.join(initargs_path, "..", "..", "..", ".."))
    dbdir = os.path.realpath(read_guts(guts, "db_dir"))
    viewdir = os.path.realpath(read_guts(guts, "idx_dir"))
    nodes = correct_split(read_guts(guts, "nodes"), ",")

    diag_url = "http://%s:%s/diag?noLogs=1" % (local_url_addr, read_guts(guts, "rest_port"))
    if not options.multi_node_diag:
        diag_url += "&oneNode=1"


    from distutils.spawn import find_executable

    lookup_cmd = None
    for cmd in ["dig", "nslookup", "host"]:
        if find_executable(cmd) is not None:
            lookup_cmd = cmd
            break

    lookup_tasks = []
    if lookup_cmd is not None:
        lookup_tasks = [UnixTask("DNS lookup information for %s" % node,
                                 "%(lookup_cmd)s '%(node)s'" % locals())
                        for node in nodes]

    getent_tasks = [LinuxTask("Name Service Switch "
                              "hosts database info for %s" % node,
                              ["getent", "ahosts", node])
                    for node in nodes]

    query_tasks = []
    query_port = read_guts(guts, "query_port")
    if query_port:
        def make(statement):
            return make_query_task(statement, user="@",
                                   password=memcached_pass,
                                   port=query_port)

        query_tasks = [make("SELECT * FROM system:datastores"),
                       make("SELECT * FROM system:namespaces"),
                       make("SELECT * FROM system:keyspaces"),
                       make("SELECT * FROM system:indexes")]

    index_tasks = []
    index_port = read_guts(guts, "indexer_http_port")
    if index_port:
        index_tasks = [make_index_task("Index definitions are: ", "getIndexStatus",
                                       memcached_pass, index_port),
                       make_index_task("Indexer settings are: ", "settings",
                                       memcached_pass, index_port),
                       make_index_task("Index storage stats are: ", "stats/storage",
                                       memcached_pass, index_port),
                       make_index_task("MOI allocator stats are: ", "stats/storage/mm",
                                       memcached_pass, index_port),
                       make_index_task("Indexer Go routine dump: ", "debug/pprof/goroutine?debug=1",
                                       memcached_pass, index_port, logfile="indexer_pprof.log"),
                       make_index_task("Indexer Rebalance Tokens: ", "listRebalanceTokens",
                                       memcached_pass, index_port),
                       make_index_task("Indexer Metadata Tokens: ", "listMetadataTokens",
                                       memcached_pass, index_port),
                       make_index_task("Indexer Memory Profile: ", "debug/pprof/heap?debug=1",
                                       memcached_pass, index_port, logfile="indexer_mprof.log"),
        ]

    projector_tasks = []
    proj_port = read_guts(guts, "projector_port")
    if proj_port:
        proj_url = 'http://%s:%s/debug/pprof/goroutine?debug=1' % (local_url_addr, proj_port)
        projector_tasks = [make_curl_task(name="Projector Go routine dump ",
                                          user="@", password=memcached_pass,
                                          url=proj_url, log_file="projector_pprof.log")]

    fts_tasks = []
    fts_port = read_guts(guts, "fts_http_port")
    if fts_port:
        url = 'http://%s:%s/api/diag' % (local_url_addr, fts_port)
        fts_tasks = [make_curl_task(name="FTS /api/diag: ",
                                    user="@", password=memcached_pass,
                                    url=url,
                                    log_file="fts_diag.json", no_header=True)]

    cbas_tasks = []
    cbas_port = read_guts(guts, "cbas_admin_port")
    if cbas_port:
        url = 'http://%s:%s/analytics/node/diagnostics' % (local_url_addr, cbas_port)
        cbas_tasks = [make_curl_task(name="CBAS /analytics/node/diagnostics: ",
                                     user="@", password=memcached_pass,
                                     url=url,
                                     log_file="analytics_diag.json", no_header=True)]

    _tasks = [
        UnixTask("Directory structure",
                 ["ls", "-lRai", root]),
        UnixTask("Database directory structure",
                 ["ls", "-lRai", dbdir]),
        UnixTask("Index directory structure",
                 ["ls", "-lRai", viewdir]),
        UnixTask("couch_dbinfo",
                 ["find", dbdir, "-type", "f",
                  "-name", "*.couch.*",
                  "-exec", "couch_dbinfo", "{}", "+"]),
        LinuxTask("Database directory filefrag info",
                  ["find", dbdir, "-type", "f", "-exec", "filefrag", "-v", "{}", "+"]),
        LinuxTask("Index directory filefrag info",
                  ["find", viewdir, "-type", "f", "-exec", "filefrag", "-v", "{}", "+"]),
        WindowsTask("Database directory structure",
                    "dir /s " + winquote_path(dbdir)),
        WindowsTask("Index directory structure",
                    "dir /s " + winquote_path(viewdir)),
        WindowsTask("Version file",
                    "type " + winquote_path(basedir()) + "\\..\\VERSION.txt"),
        WindowsTask("Manifest file",
                    "type " + winquote_path(basedir()) + "\\..\\manifest.txt"),
        WindowsTask("Manifest file",
                    "type " + winquote_path(basedir()) + "\\..\\manifest.xml"),
        LinuxTask("Version file", "cat '%s/VERSION.txt'" % root),
        LinuxTask("Variant file", "cat '%s/VARIANT.txt'" % root),
        LinuxTask("Manifest file", "cat '%s/manifest.txt'" % root),
        LinuxTask("Manifest file", "cat '%s/manifest.xml'" % root),
        LiteralTask("Couchbase config", read_guts(guts, "ns_config")),
        LiteralTask("Couchbase static config", read_guts(guts, "static_config")),
        LiteralTask("Raw ns_log", read_guts(guts, "ns_log")),
        # TODO: just gather those in python
        WindowsTask("Memcached logs",
                    "cd " + winquote_path(read_guts(guts, "memcached_logs_path")) + " && " +
                    "for /f %a IN ('dir memcached.log.* /od /tw /b') do type %a",
                    log_file="memcached.log"),
        UnixTask("Memcached logs",
                 ["sh", "-c", 'cd "$1"; for file in $(ls -tr memcached.log.*); do cat \"$file\"; done', "--", read_guts(guts, "memcached_logs_path")],
                 log_file="memcached.log"),
        [WindowsTask("Ini files (%s)" % p,
                     "type " + winquote_path(p),
                     log_file="ini.log")
         for  p in read_guts(guts, "couch_inis").split(";")],
        UnixTask("Ini files",
                 ["sh", "-c", 'for i in "$@"; do echo "file: $i"; cat "$i"; done', "--"] + read_guts(guts, "couch_inis").split(";"),
                 log_file="ini.log"),

        make_curl_task(name="couchbase diags",
                       user="@",
                       password=memcached_pass,
                       timeout=600,
                       url=diag_url,
                       log_file="diag.log"),

        make_curl_task(name="master events",
                       user="@",
                       password=memcached_pass,
                       timeout=300,
                       url='http://%s:%s/diag/masterEvents?o=1' % (local_url_addr, read_guts(guts, "rest_port")),
                       log_file="master_events.log",
                       no_header=True),

        make_curl_task(name="ale configuration",
                       user="@",
                       password=memcached_pass,
                       url='http://%s:%s/diag/ale' % (local_url_addr, read_guts(guts, "rest_port")),
                       log_file="couchbase.log"),

        [AllOsTask("couchbase logs (%s)" % name, "cbbrowse_logs %s" % name,
                   addenv = [("REPORT_DIR", read_guts(guts, "log_path"))],
                   log_file="ns_server.%s" % name)
         for name in ["debug.log", "info.log", "error.log", "couchdb.log",
                      "xdcr_target.log",
                      "views.log", "mapreduce_errors.log",
                      "stats.log", "babysitter.log",
                      "reports.log", "http_access.log",
                      "http_access_internal.log", "ns_couchdb.log",
                      "goxdcr.log", "query.log", "projector.log", "indexer.log",
                      "fts.log", "metakv.log", "json_rpc.log", "eventing.log",
                      "analytics.log", "analytics_cbas.log", "analytics_shutdown.log", "analytics_trace.json"]],

        [make_cbstats_task(kind, memcached_pass, guts)
         for kind in ["all", "allocator", "checkpoint", "config",
                      "dcp", "dcpagg",
                      ["diskinfo", "detail"], ["dispatcher", "logs"],
                      "failovers", ["hash", "detail"],
                      "kvstore", "kvtimings", "memory",
                      "prev-vbucket",
                      "runtimes", "scheduler",
                      "tasks",
                      "timings", "uuid",
                      "vbucket", "vbucket-details", "vbucket-seqno",
                      "warmup", "workload"]],

        [AllOsTask("memcached mcstat %s" % kind,
                   flatten(["mcstat", "-h", "%s:%s" % (local_url_addr, read_guts(guts, "memcached_port")),
                            "-u", read_guts(guts, "memcached_admin"), kind]),
                   log_file="stats.log",
                   timeout=60,
                   addenv=[("CB_PASSWORD", memcached_pass)])
         for kind in ["connections", "tracing"]],

        [AllOsTask("fts mossScope (%s)" % path,
                   ["mossScope", "stats", "diag", path],
                   log_file="fts_mossScope_stats.log")
         for path in glob.glob(os.path.join(viewdir, "@fts", "*.pindex", "store"))],

        [AllOsTask("ddocs for %s (%s)" % (bucket, path),
                   ["couch_dbdump", path],
                   log_file = "ddocs.log")
         for bucket in set(correct_split(read_guts(guts, "buckets"), ",")) - set(correct_split(read_guts(guts, "memcached_buckets"), ","))
         for path in glob.glob(os.path.join(dbdir, bucket, "master.couch*"))],

        [AllOsTask("Couchstore local documents (%s, %s)" % (bucket, os.path.basename(path)),
                   ["couch_dbdump", "--local", path],
                   log_file = "couchstore_local.log")
        for bucket in set(correct_split(read_guts(guts, "buckets"), ",")) - set(correct_split(read_guts(guts, "memcached_buckets"), ","))
        for path in glob.glob(os.path.join(dbdir, bucket, "*.couch.*"))],

        # RocksDB has logs per DB (i.e. vBucket). 'LOG' is the most
        # recent file, with old files named LOG.old.<timestamp>.
        # Sort so we go from oldest -> newest as per other log files.
        [AllOsTask("RocksDB Log file (%s, %s)" % (bucket, os.path.basename(path)),
                   "cat '%s'" % (log_file),
                   log_file="kv_rocks.log")
        for bucket in (set(correct_split(read_guts(guts, "buckets"), ",")) -
                       set(correct_split(read_guts(guts, "memcached_buckets"), ",")))
        for path in glob.glob(os.path.join(dbdir, bucket, "rocksdb.*"))
        for log_file in sorted(glob.glob(os.path.join(path, "LOG.old.*"))) + [os.path.join(path, "LOG")]],

        [UnixTask("moxi stats (port %s)" % port,
                  "echo stats proxy | nc %s %s" % (local_addr, port),
                  log_file="stats.log",
                  timeout=60)
         for port in correct_split(read_guts(guts, "moxi_ports"), ",")],

        [AllOsTask("mctimings %s" % stat,
                   ["mctimings",
                    "-u", read_guts(guts, "memcached_admin"),
                    "-h", "%s:%s" % (local_url_addr, read_guts(guts, "memcached_port")),
                    "-v"] + stat,
                   log_file="stats.log",
                   timeout=60,
                   addenv=[("CB_PASSWORD", memcached_pass)])
         for stat in ([], ["subdoc_execute"])],

        CollectFile("Users storage", read_guts(guts, "users_storage_path")),

        make_stats_archives_task(guts, initargs_path),
        AllOsTask("Phosphor Trace",
                  ["kv_trace_dump",
                   "-H", "%s:%s" % (local_url_addr, read_guts(guts, "memcached_port")),
                   "-u", read_guts(guts, "memcached_admin"),
                   "-P", memcached_pass,
                   "kv_trace.json"],
                  timeout=120,
                  log_file="stats.log",
                  change_dir=True,
                  artifacts=["kv_trace.json"]),
        ]

    _tasks = flatten([getent_tasks, lookup_tasks, query_tasks, index_tasks,
                      projector_tasks, fts_tasks, cbas_tasks, _tasks])

    return _tasks

def find_script(name):
    dirs = [basedir(), os.path.join(basedir(), "scripts")]
    for d in dirs:
        path = os.path.join(d, name)
        if os.path.exists(path):
            log("Found %s: %s" % (name, path))
            return os.path.abspath(path)

    return None

def get_server_guts(initargs_path):
    dump_guts_path = find_script("dump-guts")

    if dump_guts_path is None:
        log("Couldn't find dump-guts script. Some information will be missing")
        return {}

    escript = exec_name("escript")
    extra_args = os.getenv("EXTRA_DUMP_GUTS_ARGS")
    args = [escript, dump_guts_path, "--initargs-path", initargs_path]
    if extra_args:
        args = args + extra_args.split(";")
    print("Checking for server guts in %s..." % initargs_path)
    p = subprocess.Popen(args, stdout = subprocess.PIPE)
    output = p.stdout.read()
    p.wait()
    rc = p.returncode
    # print("args: %s gave rc: %d and:\n\n%s\n" % (args, rc, output))
    tokens = output.rstrip("\0").split("\0")
    d = {}
    if len(tokens) > 1:
        for i in xrange(0, len(tokens), 2):
            d[tokens[i]] = tokens[i+1]
    return d

def guess_utility(command):
    if isinstance(command, list):
        command = ' '.join(command)

    if not command:
        return None

    if re.findall(r'[|;&]|\bsh\b|\bsu\b|\bfind\b|\bfor\b', command):
        # something hard to easily understand; let the human decide
        return command
    else:
        return command.split()[0]

def dump_utilities(*args, **kwargs):
    specific_platforms = { SolarisTask : 'Solaris',
                           LinuxTask :  'Linux',
                           WindowsTask : 'Windows',
                           MacOSXTask : 'Mac OS X' }
    platform_utils = dict((name, set()) for name in specific_platforms.values())

    class FakeOptions(object):
        def __getattr__(self, name):
            return None

    tasks = make_os_tasks() + make_product_task({}, "", "", FakeOptions())

    for task in tasks:
        utility = guess_utility(task.command)
        if utility is None:
            continue

        for (platform, name) in specific_platforms.items():
            if isinstance(task, platform):
                platform_utils[name].add(utility)

    print '''This is an autogenerated, possibly incomplete and flawed list
of utilites used by cbcollect_info'''

    for (name, utilities) in sorted(platform_utils.items(), key=lambda x: x[0]):
        print "\n%s:" % name

        for utility in sorted(utilities):
            print "        - %s" % utility

    sys.exit(0)

def stdin_watcher():
    fd = sys.stdin.fileno()

    while True:
        try:
            buf = os.read(fd, 1024)
            # stdin closed
            if not buf:
                break

            if buf.find('\n') != -1:
                break
        except OSError, e:
            if e.errno != errno.EINTR:
                raise

def setup_stdin_watcher():
    def _in_thread():
        try:
            stdin_watcher()
        finally:
            AltExit.exit(2)
    th = threading.Thread(target = _in_thread)
    th.setDaemon(True)
    th.start()

class CurlKiller:
    def __init__(self, p):
        self.p = p
    def cleanup(self):
        if self.p != None:
            print("Killing curl...")
            os.kill(self.p.pid, signal.SIGKILL)
            print("done")
    def disarm(self):
        self.p = None

def do_upload_and_exit(path, url, proxy):
    output_fd, output_file = tempfile.mkstemp()
    os.close(output_fd)

    AltExit.register(lambda: os.unlink(output_file))

    args = ["curl", "-sS",
            "--output", output_file,
            "--proxy", proxy,
            "--write-out", "%{http_code}", "--upload-file", path, url]
    AltExit.lock.acquire()
    try:
        p = subprocess.Popen(args, stdout=subprocess.PIPE)
        k = CurlKiller(p)
        AltExit.register_and_unlock(k.cleanup)
    except Exception, e:
        AltExit.lock.release()
        raise e

    stdout, _ = p.communicate()
    k.disarm()

    if p.returncode != 0:
        sys.exit(1)
    else:
        if stdout.strip() == '200':
            log('Upload path is: %s' % url)
            log('Done uploading')
            sys.exit(0)
        else:
            log('HTTP status code: %s' % stdout)
            sys.exit(1)

def parse_host(host):
    url = urlparse.urlsplit(host)
    if not url.scheme:
        url = urlparse.urlsplit('https://' + host)

    return url.scheme, url.netloc, url.path

def generate_upload_url(parser, options, zip_filename):
    upload_url = None
    if options.upload_host:
        if not options.upload_customer:
            parser.error("Need --customer when --upload-host is given")

        scheme, netloc, path = parse_host(options.upload_host)

        customer = urllib.quote(options.upload_customer)
        fname = urllib.quote(os.path.basename(zip_filename))
        if options.upload_ticket:
            full_path = '%s/%s/%d/%s' % (path, customer, options.upload_ticket, fname)
        else:
            full_path = '%s/%s/%s' % (path, customer, fname)

        upload_url = urlparse.urlunsplit((scheme, netloc, full_path, '', ''))
        log("Will upload collected .zip file into %s" % upload_url)
    return upload_url

def check_ticket(option, opt, value):
    if re.match('^\d{1,7}$', value):
        return int(value)
    else:
        raise optparse.OptionValueError(
            "option %s: invalid ticket number: %r" % (opt, value))

class CbcollectInfoOptions(optparse.Option):
    from copy import copy

    TYPES = optparse.Option.TYPES + ("ticket",)
    TYPE_CHECKER = copy(optparse.Option.TYPE_CHECKER)
    TYPE_CHECKER["ticket"] = check_ticket

def main():
    # ask all tools to use C locale (MB-12050)
    os.environ['LANG'] = 'C'
    os.environ['LC_ALL'] = 'C'

    mydir = os.path.dirname(sys.argv[0])
    #(MB-8239)erl script fails in OSX as it is unable to find COUCHBASE_TOP -ravi
    if platform.system() == 'Darwin':
        os.environ["COUCHBASE_TOP"] = os.path.abspath(os.path.join(mydir, ".."))

    parser = optparse.OptionParser(usage=USAGE, option_class=CbcollectInfoOptions)
    parser.add_option("-r", dest="root",
                      help="root directory - defaults to %s" % (mydir + "/.."),
                      default=os.path.abspath(os.path.join(mydir, "..")))
    parser.add_option("-v", dest="verbosity", help="increase verbosity level",
                      action="count", default=0)
    parser.add_option("-p", dest="product_only", help="gather only product related information",
                      action="store_true", default=False)
    parser.add_option("-d", action="callback", callback=dump_utilities,
                      help="dump a list of commands that cbcollect_info needs")
    parser.add_option("--watch-stdin", dest="watch_stdin",
                      action="store_true", default=False,
                      help=optparse.SUPPRESS_HELP)
    parser.add_option("--initargs", dest="initargs", help="server 'initargs' path")
    parser.add_option("--multi-node-diag", dest="multi_node_diag",
                      action="store_true", default=False,
                      help="collect per-node diag  on all reachable nodes (default is just this node)")
    parser.add_option("--log-redaction-level", dest="redact_level",
                      default="none",
                      help="redaction level for the logs collected, none and partial supported (default is none)")
    parser.add_option("--log-redaction-salt", dest="salt_value",
                      default=str(uuid.uuid4()),
                      help="Is used to salt the hashing of tagged data, \
                            defaults to random uuid. If input by user it should \
                            be provided along with --log-redaction-level option")
    parser.add_option("--just-upload-into", dest="just_upload_into",
                      help=optparse.SUPPRESS_HELP)
    parser.add_option("--upload-host", dest="upload_host",
                      help="gather diagnostics and upload it for couchbase support. Gives upload host")
    parser.add_option("--customer", dest="upload_customer",
                      help="specifies customer name for upload")
    parser.add_option("--ticket", dest="upload_ticket", type='ticket',
                      help="specifies support ticket number for upload")
    parser.add_option("--bypass-sensitive-data", dest="bypass_sensitive_data",
                      action="store_true", default=False,
                      help="do not collect sensitive data")
    parser.add_option("--task-regexp", dest="task_regexp",
                      default="",
                      help="Run only tasks matching regexp. For debugging purposes only.")
    parser.add_option("--tmp-dir", dest="tmp_dir", default=None,
                      help="set the temp dir used while processing collected data. Overrides the TMPDIR env variable if set")
    parser.add_option("--upload-proxy", dest="upload_proxy", default="",
                      help="specifies proxy for upload")
    options, args = parser.parse_args()

    if len(args) != 1:
        parser.error("incorrect number of arguments. Expecting filename to collect diagnostics into")

    if options.watch_stdin:
        setup_stdin_watcher()

    zip_filename = args[0]
    if zip_filename[-4:] != '.zip':
        zip_filename = zip_filename + '.zip'

    zip_dir = os.path.dirname(os.path.abspath(zip_filename))

    if not os.access(zip_dir, os.W_OK | os.X_OK):
        print("do not have write access to the directory %s" % (zip_dir))
        sys.exit(1)

    if options.redact_level != "none" and options.redact_level != "partial":
        parser.error("Invalid redaction level. Only 'none' and 'partial' are supported.")

    redact_zip_file = zip_filename[:-4] + "-redacted" + zip_filename[-4:]
    upload_url = ""
    if options.redact_level != "none":
        upload_url = generate_upload_url(parser, options, redact_zip_file)
    else:
        upload_url = generate_upload_url(parser, options, zip_filename)


    erldir = os.path.join(mydir, 'erlang', 'bin')
    if os.name == 'posix':
        path = [mydir,
                '/opt/couchbase/bin',
                erldir,
                os.environ['PATH'],
                '/bin',
                '/sbin',
                '/usr/bin',
                '/usr/sbin']
        os.environ['PATH'] = ':'.join(path)

        library_path = [os.path.join(options.root, 'lib')]

        current_library_path = os.environ.get('LD_LIBRARY_PATH')
        if current_library_path is not None:
            library_path.append(current_library_path)

        os.environ['LD_LIBRARY_PATH'] = ':'.join(library_path)
    elif os.name == 'nt':
      path = [mydir, erldir, os.environ['PATH']]
      os.environ['PATH'] = ';'.join(path)

    if options.just_upload_into != None:
        do_upload_and_exit(args[0], options.just_upload_into,
                options.upload_proxy)

    runner = TaskRunner(verbosity=options.verbosity,
                        task_regexp=options.task_regexp,
                        tmp_dir=options.tmp_dir,
                        salt_value=options.salt_value)
    runner.run(make_redaction_task()) # We want this at the top of couchbase.log

    if not options.product_only:
        runner.run_tasks(make_os_tasks())

    initargs_variants = [os.path.abspath(os.path.join(options.root, "var", "lib", "couchbase", "initargs")),
                         "/opt/couchbase/var/lib/couchbase/initargs",
                         os.path.expanduser("~/Library/Application Support/Couchbase/var/lib/couchbase/initargs")]

    if options.initargs != None:
        initargs_variants = [options.initargs]

    guts = None
    guts_initargs_path = None

    for initargs_path in initargs_variants:
        d = get_server_guts(initargs_path)
        # print("for initargs: %s got:\n%s" % (initargs_path, d))
        if len(d) > 0:
            guts = d
            guts_initargs_path = os.path.abspath(initargs_path)
            break

    if guts is None:
        log("Couldn't read server guts. Using some default values.")

        prefix = None
        if platform.system() == 'Windows':
            prefix = 'c:/Program Files/Couchbase/Server'
        elif platform.system() == 'Darwin':
            prefix = '~/Library/Application Support/Couchbase'
        else:
            prefix = '/opt/couchbase'

        guts = {"db_dir" : os.path.join(prefix, "var/lib/couchbase/data"),
                "idx_dir" : os.path.join(prefix, "var/lib/couchbase/data"),
                "ns_log_path" : os.path.join(prefix, "var/lib/couchbase/ns_log"),
                "log_path" : os.path.join(prefix, "var/lib/couchbase/logs"),
                "memcached_logs_path" : os.path.join(prefix, "var/lib/couchbase/logs")}

        guts_initargs_path = os.path.abspath(prefix)

    ipv6 = read_guts(guts, "ipv6") == "true"
    set_local_addr(ipv6)

    memcached_password =  get_diag_password(guts)

    zip_node = read_guts(guts, "node")
    runner.literal("product diag header",
                   "Found server initargs at %s (%d)" % (guts_initargs_path, len(guts)))

    runner.run_tasks(make_product_task(guts, guts_initargs_path,
                                       memcached_password, options))

    # Collect breakpad crash dumps.
    if options.bypass_sensitive_data:
        log("Bypassing Sensitive Data: Breakpad crash dumps")
    else:
        memcached_breakpad_minidump_dir = read_guts(guts, "memcached_breakpad_minidump_dir")
        for dump in glob.glob(os.path.join(memcached_breakpad_minidump_dir, "*.dmp")):
            runner.collect_file(dump)

        # Collect indexer breakpad minidumps
        index_port = read_guts(guts, "indexer_http_port")
        if index_port:
            indexer_breakpad_minidump_dir = read_guts(guts, "indexer_breakpad_minidump_dir")
            if memcached_breakpad_minidump_dir != indexer_breakpad_minidump_dir:
                for dump in glob.glob(os.path.join(indexer_breakpad_minidump_dir, "*.dmp")):
                    runner.collect_file(dump)

    addr = zip_node.split("@")[-1]
    if addr == "127.0.0.1" or addr == "::1":
        zip_node = '@'.join(zip_node.split("@")[:-1] + [find_primary_addr(ipv6, addr)])

    if options.verbosity:
        log("Python version: %s" % sys.version)

    runner.literal("cbcollect_info log", log_stream.getvalue(),
                   log_file="cbcollect_info.log", no_header=True)

    runner.close_all_files()

    if options.redact_level != "none":
        log("Redacting log files to level: %s" % options.redact_level)
        runner.redact_and_zip(redact_zip_file, zip_node)

    runner.zip(zip_filename, zip_node)

    if upload_url and options.redact_level != "none":
        do_upload_and_exit(redact_zip_file, upload_url, options.upload_proxy)
    elif upload_url:
        do_upload_and_exit(zip_filename, upload_url, options.upload_proxy)

def find_primary_addr(ipv6, default = None):
    Family = socket.AF_INET6 if ipv6 else socket.AF_INET
    DnsAddr = "2001:4860:4860::8844" if ipv6 else "8.8.8.8"
    s = socket.socket(Family, socket.SOCK_DGRAM)
    try:
        s.connect((DnsAddr, 56))
        if ipv6:
            addr, port, _, _ = s.getsockname()
        else:
            addr, port = s.getsockname()

        return addr
    except socket.error:
        return default
    finally:
        s.close()

def exec_name(name):
    if sys.platform == 'win32':
        name += ".exe"
    return name

if __name__ == '__main__':
    main()
