package main

import (
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"time"
)

func makeOSTasks() []SGCollectTask {
	return []SGCollectTask{
		OSTask("unix", "uname", "uname -a"),
		OSTask("unix", "time and TZ", "date; date -u"),
		Timeout(OSTask("unix", "ntp time", "ntpdate -q pool.ntp.org || nc time.nist.gov 13 || netcat time.nist.gov 13"), 60*time.Second),
		MayFail(OSTask("unix", "ntp peers", "ntpq -p")),                                 // ntpq is not present on macOS Mojave and above
		MayFail(OSTask("unix", "raw /etc/sysconfig/clock", "cat /etc/sysconfig/clock")), // /etc/sysconfig/clock may not be present on macOS
		MayFail(OSTask("unix", "raw /etc/timezone", "cat /etc/timezone")),               // /etc/sysconfig/clock may not be present on macOS
		OSTask("windows", "System information", "systeminfo"),
		OSTask("windows", "Computer system", "wmic computersystem"),
		OSTask("windows", "Computer OS", "wmic os"),
		OSTask("linux", "System Hardware", "lshw -json || lshw"),
		OSTask("solaris", "Process list snapshot", "prstat -a -c -n 100 -t -v -L 1 10"),
		OSTask("solaris", "Process list", "ps -ef"),
		OSTask("solaris", "Service configuration", "svcs -a"),
		OSTask("solaris", "Swap configuration", "swap -l"),
		OSTask("solaris", "Disk activity", "zpool iostat 1 10"),
		OSTask("solaris", "Disk activity", "iostat -E 1 10"),
		OSTask("linux", "Process list snapshot", "export TERM=''; top -Hb -n1 || top -H n1"),
		OSTask("linux", "Process list", "ps -AwwL -o user,pid,lwp,ppid,nlwp,pcpu,maj_flt,min_flt,pri,nice,vsize,rss,tty,stat,wchan:12,start,bsdtime,command"),
		OSTask("linux", "Raw /proc/vmstat", "cat /proc/vmstat"),
		OSTask("linux", "Raw /proc/mounts", "cat /proc/mounts"),
		OSTask("linux", "Raw /proc/partitions", "cat /proc/partitions"),
		Sample(OSTask("linux", "Raw /proc/diskstats", "cat /proc/diskstats"), 10, time.Second),
		OSTask("linux", "Raw /proc/interrupts", "cat /proc/interrupts"),
		OSTask("linux", "Swap configuration", "free -t"),
		OSTask("linux", "Swap configuration", "swapon -s"),
		OSTask("linux", "Kernel modules", "lsmod"),
		OSTask("linux", "Distro version", "cat /etc/redhat-release"),
		OSTask("linux", "Distro version", "lsb_release -a"),
		OSTask("linux", "Distro version", "cat /etc/SuSE-release"),
		OSTask("linux", "Distro version", "cat /etc/issue"),
		OSTask("linux", "Installed software", "rpm -qa"),
		OSTask("linux", "Installed software", "COLUMNS=300 dpkg -l"),
		OSTask("linux", "Extended iostat", "iostat -x -p ALL 1 10 || iostat -x 1 10"),
		OSTask("linux", "Core dump settings", "find /proc/sys/kernel -type f -name '*core*' -print -exec cat '{}' ';'"),
		OSTask("unix", "sysctl settings", "sysctl -a"),
		OSTask("linux", "lsof output", "echo sync_gateway | xargs -n1 pgrep | xargs -n1 -r -- lsof -n -p"),
		OSTask("linux", "LVM info", "lvdisplay"),
		OSTask("linux", "LVM info", "vgdisplay"),
		OSTask("linux", "LVM info", "pvdisplay"),
		OSTask("darwin", "Process list snapshot", "top -l 1"),
		OSTask("darwin", "Disk activity", "iostat 1 10"),
		OSTask("darwin", "Process list", "ps -Aww -o user,pid,ppid,pcpu,pri,nice,vsize,rss,tty,stat,start,command"),
		OSTask("windows", "Installed software", "wmic product get name, version"),
		OSTask("windows", "Service list", "wmic service where state=\"running\" GET caption, name, state"),
		OSTask("windows", "Process list", "wmic process"),
		OSTask("windows", "Process usage", "tasklist /V /fo list"),
		OSTask("windows", "Swap settings", "wmic pagefile"),
		OSTask("windows", "Disk partition", "wmic partition"),
		OSTask("windows", "Disk volumes", "wmic volume"),
		Sample(OSTask("unix", "Network configuration", "ifconfig -a"), 2, 10*time.Second),
		OSTask("linux", "Network configuration", "echo link addr neigh rule route netns | xargs -n1 -- sh -x -c 'ip $1 list' --"),
		Sample(OSTask("windows", "Network configuration", "ipconfig /all"), 2, 10*time.Second),
		OSTask("linux", "Raw /proc/net/dev", "cat /proc/net/dev"),
		OSTask("linux", "Network link statistics", "ip -s link"),
		OSTask("unix", "Network status", "netstat -anp || netstat -an"),
		OSTask("windows", "Network status", "netstat -ano"),
		OSTask("unix", "Network routing table", "netstat -rn"),
		OSTask("linux", "Network socket statistics", "ss -an"),
		OSTask("linux", "Extended socket statistics", "ss -an --info --processes"),
		OSTask("unix", "Arp cache", "arp -na"),
		OSTask("linux", "Iptables dump", "iptables-save"),
		OSTask("unix", "Raw /etc/hosts", "cat /etc/hosts"),
		OSTask("unix", "Raw /etc/resolv.conf", "cat /etc/resolv.conf"),
		OSTask("linux", "Raw /etc/nsswitch.conf", "cat /etc/nsswitch.conf"),
		OSTask("windows", "Arp cache", "arp -a"),
		OSTask("windows", "Network Interface Controller", "wmic nic"),
		OSTask("windows", "Network Adapter", "wmic nicconfig"),
		OSTask("windows", "Active network connection", "wmic netuse"),
		OSTask("windows", "Protocols", "wmic netprotocol"),
		OSTask("windows", "Hosts file", `type %SystemRoot%\system32\drivers\etc\hosts`),
		OSTask("windows", "Cache memory", "wmic memcache"),
		OSTask("windows", "Physical memory", "wmic memphysical"),
		OSTask("windows", "Physical memory chip info", "wmic memorychip"),
		OSTask("windows", "Local storage devices", "wmic logicaldisk"),
		OSTask("unix", "Filesystem", "df -ha"),
		MayFail(OSTask("unix", "System activity reporter", "sar 1 10")),  // sar is not always installed
		MayFail(OSTask("unix", "System paging activity", "vmstat 1 10")), // vmstat is not always installed
		OSTask("unix", "System uptime", "uptime"),
		MayFail(OSTask("unix", "couchbase user definition", "getent passwd couchbase")), // might not be present in tests
		Privileged(OSTask("unix", "couchbase user limits", `su couchbase -c "ulimit -a"`)),
		MayFail(OSTask("unix", "sync_gateway user definition", "getent passwd sync_gateway")), // might not be present in tests
		Privileged(OSTask("unix", "sync_gateway user limits", `su sync_gateway -c "ulimit -a"`)),
		OSTask("linux", "Interrupt status", "intrstat 1 10"),
		OSTask("linux", "Processor status", "mpstat 1 10"),
		OSTask("solaris", "System log", "cat /var/adm/messages"),
		OSTask("linux", "Raw /proc/uptime", "cat /proc/uptime"),
		NoHeader(OverrideOutput(OSTask("linux", "Systemd journal", "journalctl 2>&1 | gzip -c"), "systemd_journal.gz")),
		NoHeader(OverrideOutput(OSTask("linux", "All logs", "tar cz /var/log/syslog* /var/log/dmesg /var/log/messages* /var/log/daemon* /var/log/debug* /var/log/kern.log* 2>/dev/null"), "syslog.tar.gz")),
		OSTask("linux", "Relevant proc data", "echo sync_gateway | xargs -n1 pgrep | xargs -n1 -- sh -c 'echo $1; cat /proc/$1/status; cat /proc/$1/limits; cat /proc/$1/smaps; cat /proc/$1/numa_maps; cat /proc/$1/task/*/sched; echo' --"),
		OSTask("linux", "Processes' environment", "echo sync_gateway | xargs -n1 pgrep | xargs -n1 -- sh -c 'echo $1; ( cat /proc/$1/environ | tr \\0 \\n ); echo' --"),
		OSTask("linux", "NUMA data", "numactl --hardware"),
		OSTask("linux", "NUMA data", "numactl --show"),
		OSTask("linux", "NUMA data", "cat /sys/devices/system/node/node*/numastat"),
		Privileged(OSTask("unix", "Kernel log buffer", "dmesg -H || dmesg")),
		OSTask("linux", "Transparent Huge Pages data", "cat /sys/kernel/mm/transparent_hugepage/enabled"),
		OSTask("linux", "Transparent Huge Pages data", "cat /sys/kernel/mm/transparent_hugepage/defrag"),
		OSTask("linux", "Transparent Huge Pages data", "cat /sys/kernel/mm/redhat_transparent_hugepage/enabled"),
		OSTask("linux", "Transparent Huge Pages data", "cat /sys/kernel/mm/redhat_transparent_hugepage/defrag"),
		OSTask("linux", "Network statistics", "netstat -s"),
		OSTask("linux", "Full raw netstat", "cat /proc/net/netstat"),
		OSTask("linux", "CPU throttling info", "echo /sys/devices/system/cpu/cpu*/thermal_throttle/* | xargs -n1 -- sh -c 'echo $1; cat $1' --"),
	}
}

func makeCollectLogsTasks(opts *SGCollectOptions, config ServerConfig) (result []SGCollectTask) {
	var sgLogFiles = []string{
		"sg_error",
		"sg_warn",
		"sg_info",
		"sg_debug",
		"sg_stats",
		"sync_gateway_access",
		"sync_gateway_error",
	}
	const sgLogExtensionNotRotated = ".log"
	const sgLogExtensionRotated = ".log.gz"
	var sgLogDirectories = []string{
		"/home/sync_gateway/logs",
		"/var/log/sync_gateway",
		"/Users/sync_gateway/logs",
		`C:\Program Files (x86)\Couchbase\var\lib\couchbase\logs`,
		`C:\Program Files\Couchbase\var\lib\couchbase\logs`,
		`C:\Program Files\Couchbase\Sync Gateway\var\lib\couchbase\logs`,
	}

	// Also try getting the current path from the config, in case it's not one of the defaults
	if cfgPath := config.Logging.LogFilePath; cfgPath != "" {
		// This could be a relative path
		if !filepath.IsAbs(cfgPath) {
			cfgPath = filepath.Join(opts.RootDir, cfgPath)
		}
		sgLogDirectories = append(sgLogDirectories, config.Logging.LogFilePath)
	}

	// Check every combination of directory/file, grab everything we can
	for _, dir := range sgLogDirectories {
		// Bail out if the directory doesn't exist, avoids unnecessary checks
		_, err := os.Stat(dir)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				log.Printf("WARN: failed to stat %q: %v", dir, err)
			}
			continue
		}
		for _, file := range sgLogFiles {
			// Grab the rotated files first, that way they'll be in the right order when ungzipped
			rotated, err := filepath.Glob(filepath.Join(dir, fmt.Sprintf("%s-*%s", file, sgLogExtensionRotated)))
			if err != nil {
				log.Printf("WARN: failed to glob %s in %s: %v", file, dir, err)
			} else {
				for _, rotatedFile := range rotated {
					log.Printf("Collecting rotated log file %s", rotatedFile)
					result = append(result, OverrideOutput(&GZipFileTask{
						name:      file + sgLogExtensionNotRotated,
						inputFile: rotatedFile,
					}, file+sgLogExtensionNotRotated))
				}
			}
			log.Printf("Collecting non-rotated log file %s", filepath.Join(dir, file+sgLogExtensionNotRotated))
			result = append(result, OverrideOutput(&FileTask{
				name:      file + sgLogExtensionNotRotated,
				inputFile: filepath.Join(dir, file+sgLogExtensionNotRotated),
			}, file+sgLogExtensionNotRotated))
		}
	}
	return result
}

func makeSGTasks(url *url.URL, opts *SGCollectOptions, config ServerConfig) (result []SGCollectTask) {
	binary, bootstrapConfigPath := findSGBinaryAndConfigs(url, opts)
	if binary != "" {
		result = append(result, OverrideOutput(NoHeader(&FileTask{
			name:      "Sync Gateway executable",
			inputFile: binary,
		}), "sync_gateway"))
	}
	if bootstrapConfigPath != "" {
		result = append(result, RemovePasswords(OverrideOutput(NoHeader(&FileTask{
			name:      "Sync Gateway bootstrapConfigPath",
			inputFile: bootstrapConfigPath,
		}), "sync_gateway.json")))
	}

	result = append(result, OverrideOutput(NoHeader(&URLTask{
		name: "Sync Gateway expvars",
		url:  url.String() + "/_expvar",
	}), "expvars.json"), RemovePasswords(&URLTask{
		name: "Collect server config",
		url:  url.String() + "/_config",
	}), RemovePasswords(&URLTask{
		name: "Collect runtime config",
		url:  url.String() + "/_config?include_runtime=true",
	}), RemovePasswords(&URLTask{
		name: "Collect server status",
		url:  url.String() + "/_status",
	}))
	if len(config.Databases) > 0 {
		for db := range config.Databases {
			result = append(result, RemovePasswords(&URLTask{
				name: fmt.Sprintf("Database config - %q", db),
				url:  url.String() + fmt.Sprintf("/%s/_config?include_runtime=true", db),
			}))
		}
	}
	for _, profile := range [...]string{"profile", "heap", "goroutine", "block", "mutex"} {
		result = append(result, OverrideOutput(NoHeader(&URLTask{
			name: fmt.Sprintf("Collect %s pprof", profile),
			url:  url.String() + fmt.Sprintf("/_debug/pprof/%s", profile),
			// Override timeout for pprof requests as they can take a bit longer
			timeout: durationPtr(time.Minute),
		}), fmt.Sprintf("pprof_%s.pb.gz", profile)))
	}
	result = append(result, makeCollectLogsTasks(opts, config)...)
	return
}

func MakeAllTasks(url *url.URL, opts *SGCollectOptions, config ServerConfig) []SGCollectTask {
	result := []SGCollectTask{
		new(SGCollectOptionsTask),
	}
	result = append(result, makeOSTasks()...)
	result = append(result, makeSGTasks(url, opts, config)...)
	return result
}

func durationPtr(d time.Duration) *time.Duration {
	return &d
}
