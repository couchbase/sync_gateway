package base

import "fmt"

func SetMaxFileDescriptors(maxFDs uint64) (uint64, error) {
	return 0, fmt.Errorf("Unsupported on Windows")
}
