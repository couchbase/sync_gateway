package main

import (
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/couchbase/sync_gateway/rest"
)

func makeOSTasks() []SGCollectTask {
	return []SGCollectTask{
		OSTask("unix", "uname", "uname -a"),
		OSTask("unix", "time and TZ", "date; date -u"),
		Timeout(OSTask("unix", "ntp time", "ntpdate -q pool.ntp.org || nc time.nist.gov 13 || netcat time.nist.gov 13"), 60*time.Second),
		OSTask("unix", "ntp peers", "ntpq -p"),
		OSTask("unix", "raw /etc/sysconfig/clock", "cat /etc/sysconfig/clock"),
		OSTask("unix", "raw /etc/timezone", "cat /etc/timezone"),
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
	}
}

func makeCollectLogsTasks(opts *SGCollectOptions, config rest.RunTimeServerConfigResponse) (result []SGCollectTask) {
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
	if config.StartupConfig != nil {
		if cfgPath := config.Logging.LogFilePath; cfgPath != "" {
			// This could be a relative path
			if !filepath.IsAbs(cfgPath) {
				cfgPath = filepath.Join(opts.RootDir, cfgPath)
			}
			sgLogDirectories = append(sgLogDirectories, config.Logging.LogFilePath)
		}
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
					result = append(result, &GZipFileTask{
						name:       file + sgLogExtensionNotRotated,
						inputFile:  rotatedFile,
						outputFile: file + sgLogExtensionNotRotated,
					})
				}
			}
			result = append(result, &FileTask{
				name:       file + sgLogExtensionNotRotated,
				inputFile:  filepath.Join(dir, file+sgLogExtensionNotRotated),
				outputFile: file + sgLogExtensionNotRotated,
			})
		}
	}
	return result
}

func makeSGTasks(url *url.URL, opts *SGCollectOptions, config rest.RunTimeServerConfigResponse) (result []SGCollectTask) {
	binary, bootstrapConfigPath := findSGBinaryAndConfigs(url, opts)
	if binary != "" {
		result = append(result, &FileTask{
			name:       "Sync Gateway executable",
			inputFile:  binary,
			outputFile: "sync_gateway",
		})
	}
	if bootstrapConfigPath != "" {
		result = append(result, &FileTask{
			name:       "Sync Gateway bootstrapConfigPath",
			inputFile:  bootstrapConfigPath,
			outputFile: "sync_gateway.json",
		})
	}

	result = append(result, &URLTask{
		name:       "Sync Gateway expvars",
		url:        url.String() + "/_expvar",
		outputFile: "expvars.json",
	}, &URLTask{
		name: "Collect server bootstrapConfigPath",
		url:  url.String() + "/_config",
	}, &URLTask{
		name: "Collect runtime bootstrapConfigPath",
		url:  url.String() + "/_config?include_runtime=true",
	}, &URLTask{
		name: "Collect server status",
		url:  url.String() + "/_status",
	})
	result = append(result, makeCollectLogsTasks(opts, config)...)
	return
}
