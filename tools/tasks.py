#!/usr/bin/env python
# -*- python -*-
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
import urllib2
import base64
import mmap

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


def log(message, end = '\n'):
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
            print >> fp, self.literal
            return 0

        env = None
        if "addenv" in self.__dict__:
            env = os.environ.copy()
            env.update(self.addenv)
        try:
            p = subprocess.Popen(self.command, bufsize=-1,
                                 stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT,
                                 shell=use_shell, env=env)
        except OSError, e:
            # if use_shell is False then Popen may raise exception
            # if binary is missing. In this case we mimic what
            # shell does. Namely, complaining to stderr and
            # setting non-zero status code. It's might also
            # automatically handle things like "failed to fork due
            # to some system limit".
            print >> fp, "Failed to execute %s: %s" % (self.command, e)
            return 127
        p.stdin.close()

        from threading import Timer, Event

        timer = None
        timer_fired = Event()

        if self.timeout is not None and hasattr(p, 'kill'):
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
                    print >> fp, "`%s` timed out after %s seconds" % (self.command, self.timeout)

        return p.wait()

    def will_run(self):
        """Determine if this task will run on this platform."""
        return sys.platform in self.platforms


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
        self.__dict__.update(kwargs)

    def execute(self, fp):
        """Run the task"""
        print("log_file: {}. ".format(self.log_file))
        try:
            result = self.callable()
            fp.write(result)
            return 0
        except Exception as e:
            if self.log_exception:
                print("Exception executing python task: {}".format(e))
            return 1

    def will_run(self):
        """Determine if this task will run on this platform."""
        return True


class TaskRunner(object):

    def __init__(self, verbosity=0, default_name="couchbase.log"):
        self.files = {}
        self.tasks = {}
        self.verbosity = verbosity
        self.start_time = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
        self.tmpdir = tempfile.mkdtemp()
        self.default_name = default_name
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
            self.files[filename] = open(filename, 'r')
        else:
            log("Unable to collect file '{0}' - already collected.".format(
                filename))

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

    def run(self, task):
        """Run a task with a file descriptor corresponding to its log file"""
        if task.will_run():
            if hasattr(task, 'command_to_print'):
                command_to_print = task.command_to_print
            else:
                command_to_print = task.command

            log("%s (%s) - " % (task.description, command_to_print), end='')
            if task.privileged and os.getuid() != 0:
                log("skipped (needs root privs)")
                return

            if hasattr(task, 'log_file'):
                filename = task.log_file
            else:
                filename = self.default_name

            fp = self.get_file(filename)
            if not task.no_header:
                self.header(fp, task.description, command_to_print)

            for i in xrange(task.num_samples):
                if i > 0:
                    log("Taking sample %d after %f seconds - " % (i+1, task.interval), end='')
                    time.sleep(task.interval)
                result = task.execute(fp)
                self.log_result(result)
            fp.flush()

        elif self.verbosity >= 2:
            log('Skipping "%s" (%s): not for platform %s' % (task.description, command_to_print, sys.platform))

    def zip(self, filename, log_type, node):
        """Write all our logs to a zipfile"""
        exe = exec_name("gozip")

        prefix = "%s_%s_%s" % (log_type, node, self.start_time)

        files = []
        for name, fp in self.files.iteritems():
            fp.close()
            files.append(fp.name)

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


def make_curl_task(name, url, user="", password="", content_postprocessors=[],
                   timeout=60, log_file="python_curl.log",
                   **kwargs):
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
    def python_curl_task():
        r = urllib2.Request(url=url)
        if len(user) > 0:
            base64string = base64.encodestring('%s:%s' % (user, password)).replace('\n', '')
            r.add_header("Authorization", "Basic %s" % base64string)
        response_file_handle = urllib2.urlopen(r, timeout=timeout)
        response_string = response_file_handle.read()
        for content_postprocessor in content_postprocessors:
            response_string = content_postprocessor(response_string)
        return response_string

    return PythonTask(
        description=name,
        callable=python_curl_task,
        log_file=log_file,
        **kwargs
    )

def add_file_task(sourcefile_path, content_postprocessors=[]):
    """
    Adds the contents of a file to the output zip

    The content_postprocessors is a list of functions -- see make_curl_task
    """
    def python_add_file_task():
        with open(sourcefile_path, 'r') as infile:
            contents = infile.read()
            for content_postprocessor in content_postprocessors:
                contents = content_postprocessor(contents)
            return contents

    task = PythonTask(
        description="Contents of {}".format(sourcefile_path),
        callable=python_add_file_task,
        log_file=os.path.basename(sourcefile_path),
        log_exception=False,
    )

    return task

def make_query_task(statement, user, password, port):
    url = "http://127.0.0.1:%s/query/service?statement=%s" % (port, urllib.quote(statement))

    return make_curl_task(name="Result of query statement \'%s\'" % statement,
                          user=user, password=password, url=url)

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


def make_os_tasks(processes):
    programs = " ".join(processes)

    _tasks = [
        UnixTask("uname", "uname -a"),
        UnixTask("time and TZ", "date; date -u"),
        UnixTask("ntp time",
                 "ntpdate -q pool.ntp.org || "
                 "nc time.nist.gov 13 || "
                 "netcat time.nist.gov 13"),
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
        # NOTE: AFAIK columns _was_ necessary, but it doesn't appear to be
        # required anymore. I.e. dpkg -l correctly detects stdout as not a
        # tty and stops playing smart on formatting. Lets keep it for few
        # years and then drop, however.
        LinuxTask("Installed software", "COLUMNS=300 dpkg -l"),
        LinuxTask("Extended iostat", "iostat -x -p ALL 1 10 || iostat -x 1 10"),
        LinuxTask("Core dump settings", "find /proc/sys/kernel -type f -name '*core*' -print -exec cat '{}' ';'"),
        UnixTask("sysctl settings", "sysctl -a"),
        LinuxTask("Relevant lsof output",
                  "echo %(programs)s | xargs -n1 pgrep | xargs -n1 -r -- lsof -n -p" % locals()),
        LinuxTask("LVM info", "lvdisplay"),
        LinuxTask("LVM info", "vgdisplay"),
        LinuxTask("LVM info", "pvdisplay"),
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
        WindowsTask("Hosts file", "type %SystemRoot%\system32\drivers\etc\hosts"),
        WindowsTask("Cache memory", "wmic memcache"),
        WindowsTask("Physical memory", "wmic memphysical"),
        WindowsTask("Physical memory chip info", "wmic memorychip"),
        WindowsTask("Local storage devices", "wmic logicaldisk"),
        UnixTask("Filesystem", "df -ha"),
        UnixTask("System activity reporter", "sar 1 10"),
        UnixTask("System paging activity", "vmstat 1 10"),
        UnixTask("System uptime", "uptime"),
        UnixTask("couchbase user definition", "getent passwd couchbase"),
        UnixTask("couchbase user limits", "su couchbase -c \"ulimit -a\"",
                 privileged=True),
        UnixTask("couchbase user limits", "su couchbase -c \"ulimit -a\"",
                 privileged=True),
        UnixTask("Interrupt status", "intrstat 1 10"),
        UnixTask("Processor status", "mpstat 1 10"),
        UnixTask("System log", "cat /var/adm/messages"),
        LinuxTask("Raw /proc/uptime", "cat /proc/uptime"),
        LinuxTask("All logs", "tar cz /var/log/syslog* /var/log/dmesg /var/log/messages* /var/log/daemon* /var/log/debug* /var/log/kern.log* 2>/dev/null",
                  log_file="syslog.tar.gz", no_header=True),
        LinuxTask("Relevant proc data", "echo %(programs)s | "
                  "xargs -n1 pgrep | xargs -n1 -- sh -c 'echo $1; cat /proc/$1/status; cat /proc/$1/limits; cat /proc/$1/smaps; cat /proc/$1/numa_maps; cat /proc/$1/task/*/sched; echo' --" % locals()),
        LinuxTask("Processes' environment", "echo %(programs)s | "
                  r"xargs -n1 pgrep | xargs -n1 -- sh -c 'echo $1; ( cat /proc/$1/environ | tr \\0 \\n ); echo' --" % locals()),
        LinuxTask("NUMA data", "numactl --hardware"),
        LinuxTask("NUMA data", "numactl --show"),
        LinuxTask("NUMA data", "cat /sys/devices/system/node/node*/numastat"),
        UnixTask("Kernel log buffer", "dmesg -H || dmesg"),
        LinuxTask("Transparent Huge Pages data", "cat /sys/kernel/mm/transparent_hugepage/enabled"),
        LinuxTask("Transparent Huge Pages data", "cat /sys/kernel/mm/transparent_hugepage/defrag"),
        LinuxTask("Transparent Huge Pages data", "cat /sys/kernel/mm/redhat_transparent_hugepage/enabled"),
        LinuxTask("Transparent Huge Pages data", "cat /sys/kernel/mm/redhat_transparent_hugepage/defrag"),
        LinuxTask("Network statistics", "netstat -s"),
        LinuxTask("Full raw netstat", "cat /proc/net/netstat"),
        LinuxTask("CPU throttling info", "echo /sys/devices/system/cpu/cpu*/thermal_throttle/* | xargs -n1 -- sh -c 'echo $1; cat $1' --"),
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

    return AllOsTask("stats archives",
                     [escript,
                      escript_wrapper,
                      "--initargs-path", initargs_path, "--",
                      dump_stats, stats_dir],
                     no_header=True,
                     log_file="stats_archives.json")

def make_product_task(guts, initargs_path, options):
    root = os.path.abspath(os.path.join(initargs_path, "..", "..", "..", ".."))
    dbdir = read_guts(guts, "db_dir")
    viewdir = read_guts(guts, "idx_dir")

    diag_url = "http://127.0.0.1:%s/diag?noLogs=1" % read_guts(guts, "rest_port")
    if options.single_node_diag:
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
                        for node in correct_split(read_guts(guts, "nodes"), ",")]

    query_tasks = []
    query_port = read_guts(guts, "query_port")
    if query_port:
        def make(statement):
            return make_query_task(statement, user="@",
                                   password=read_guts(guts, "memcached_pass"),
                                   port=query_port)

        query_tasks = [make("SELECT * FROM system:datastores"),
                       make("SELECT * FROM system:namespaces"),
                       make("SELECT * FROM system:keyspaces"),
                       make("SELECT * FROM system:indexes")]

    index_tasks = []
    index_port = read_guts(guts, "indexer_http_port")
    if index_port:
        url='http://127.0.0.1:%s/getIndexStatus' % index_port
        index_tasks = [make_curl_task(name="Index definitions are: ",
                          user="@", password=read_guts(guts, "memcached_pass"), url=url)]

    fts_tasks = []
    fts_port = read_guts(guts, "fts_http_port")
    if fts_port:
        url='http://127.0.0.1:%s/api/diag' % fts_port
        fts_tasks = [make_curl_task(name="FTS /api/diag: ",
                       user="@", password=read_guts(guts, "memcached_pass"), url=url)]

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
        LinuxTask("Manifest file", "cat '%s/manifest.txt'" % root),
        LinuxTask("Manifest file", "cat '%s/manifest.xml'" % root),
        AllOsTask("Couchbase config", "", literal = read_guts(guts, "ns_config")),
        AllOsTask("Couchbase static config", "", literal = read_guts(guts, "static_config")),
        AllOsTask("Raw ns_log", "", literal = read_guts(guts, "ns_log")),
        # TODO: just gather those in python
        WindowsTask("Memcached logs",
                    "cd " + winquote_path(read_guts(guts, "memcached_logs_path")) + " && " +
                    "for /f %a IN ('dir /od /b memcached.log.*') do type %a",
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
                       password=read_guts(guts, "memcached_pass"),
                       timeout=600,
                       url=diag_url,
                       log_file="diag.log"),

        make_curl_task(name="master events",
                       user="@",
                       password=read_guts(guts, "memcached_pass"),
                       timeout=300,
                       url='http://127.0.0.1:%s/diag/masterEvents?o=1' % read_guts(guts, "rest_port"),
                       log_file="master_events.log",
                       no_header=True),

        make_curl_task(name="ale configuration",
                       user="@",
                       password=read_guts(guts, "memcached_pass"),
                       url='http://127.0.0.1:%s/diag/ale' % read_guts(guts, "rest_port"),
                       log_file="couchbase.log"),

        [AllOsTask("couchbase logs (%s)" % name, "cbbrowse_logs %s" % name,
                   addenv = [("REPORT_DIR", read_guts(guts, "log_path"))],
                   log_file="ns_server.%s" % name)
         for name in ["debug.log", "info.log", "error.log", "couchdb.log",
                      "xdcr.log", "xdcr_errors.log",
                      "views.log", "mapreduce_errors.log",
                      "stats.log", "babysitter.log", "ssl_proxy.log",
                      "reports.log", "xdcr_trace.log", "http_access.log",
                      "http_access_internal.log", "ns_couchdb.log",
                      "goxdcr.log", "query.log", "projector.log", "indexer.log",
                      "fts.log", "metakv.log"]],

        [AllOsTask("memcached stats %s" % kind,

                   flatten(["cbstats", "-a", "127.0.0.1:%s" % read_guts(guts, "memcached_port"), kind, "-b", read_guts(guts, "memcached_admin"), "-p", read_guts(guts, "memcached_pass")]),
                   log_file="stats.log",
                   timeout=60)
         for kind in ["all", "allocator", "checkpoint", "config",
                      "dcp", "dcpagg",
                      ["diskinfo", "detail"], ["dispatcher", "logs"],
                      "failovers", ["hash", "detail"],
                      "kvstore", "kvtimings", "memory",
                      "prev-vbucket",
                      "runtimes", "scheduler",
                      "tap", "tapagg",
                      "timings", "uuid",
                      "vbucket", "vbucket-details", "vbucket-seqno",
                      "warmup", "workload"]],

        [AllOsTask("memcached mcstat %s" % kind,
                   flatten(["mcstat", "-h", "127.0.0.1:%s" % read_guts(guts, "memcached_port"),
                            "-u", read_guts(guts, "memcached_admin"),
                            "-P", read_guts(guts, "memcached_pass"), kind]),
                   log_file="stats.log",
                   timeout=60)
         for kind in ["connections"]],

        [AllOsTask("ddocs for %s (%s)" % (bucket, path),
                   ["couch_dbdump", path],
                   log_file = "ddocs.log")
         for bucket in set(correct_split(read_guts(guts, "buckets"), ",")) - set(correct_split(read_guts(guts, "memcached_buckets"), ","))
         for path in glob.glob(os.path.join(dbdir, bucket, "master.couch*"))],
        [AllOsTask("replication docs (%s)" % (path),
                   ["couch_dbdump", path],
                   log_file = "ddocs.log")
         for path in glob.glob(os.path.join(dbdir, "_replicator.couch*"))],

        [AllOsTask("Couchstore local documents (%s, %s)" % (bucket, os.path.basename(path)),
                   ["couch_dbdump", "--local", path],
                   log_file = "couchstore_local.log")
        for bucket in set(correct_split(read_guts(guts, "buckets"), ",")) - set(correct_split(read_guts(guts, "memcached_buckets"), ","))
        for path in glob.glob(os.path.join(dbdir, bucket, "*.couch.*"))],

        [UnixTask("moxi stats (port %s)" % port,
                  "echo stats proxy | nc 127.0.0.1 %s" % port,
                  log_file="stats.log",
                  timeout=60)
         for port in correct_split(read_guts(guts, "moxi_ports"), ",")],

        [AllOsTask("mctimings",
                   ["mctimings",
                    "-u", read_guts(guts, "memcached_admin"),
                    "-P", read_guts(guts, "memcached_pass"),
                    "-h", "127.0.0.1:%s" % read_guts(guts, "memcached_port"),
                    "-v"] + stat,
                   log_file="stats.log",
                   timeout=60)
         for stat in ([], ["subdoc_execute"])],

        make_stats_archives_task(guts, initargs_path)
        ]

    _tasks = flatten([lookup_tasks, query_tasks, index_tasks, fts_tasks, _tasks])

    return _tasks

def find_script(name):
    dirs = [basedir(), os.path.join(basedir(), "scripts")]
    for d in dirs:
        path = os.path.join(d, name)
        if os.path.exists(path):
            log("Found %s: %s" % (name, path))
            return path

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

    tasks = make_os_tasks() + make_product_task({}, "", FakeOptions())

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

def setup_stdin_watcher():
    def _in_thread():
        sys.stdin.readline()
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

def do_upload_and_exit(path, url):

    f = open(path, 'rb')

    # mmap the file to reduce the amount of memory required (see bit.ly/2aNENXC)
    filedata = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
    opener = urllib2.build_opener()
    request = urllib2.Request(url.encode('utf-8'),data=filedata)
    request.add_header(str('Content-Type'), str('application/zip'))
    request.get_method = lambda: str('PUT')
    url = opener.open(request)

    exit_code = 0
    if url.getcode() == 200:
        log('Done uploading')
    else:
        log('Error uploading.  HTTP status code: %s' % url.getcode())
        exit_code = 1

    filedata.close()
    f.close()

    sys.exit(exit_code)

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

def find_primary_addr(default = None):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        try:
            s.connect(("8.8.8.8", 56))
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


