// +build !windows

package base

import "syscall"

func SetMaxFileDescriptors(maxFDs uint64) (uint64, error) {
	var limits syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limits); err != nil {
		return maxFDs, err
	}
	if maxFDs > limits.Max {
		maxFDs = limits.Max
	}
	if limits.Cur == maxFDs {
		return maxFDs, nil
	}
	limits.Cur = maxFDs
	limits.Max = maxFDs
	return maxFDs, syscall.Setrlimit(syscall.RLIMIT_NOFILE, &limits)
}
