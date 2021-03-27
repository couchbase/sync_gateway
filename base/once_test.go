package base

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOnceReset(t *testing.T) {
	var calls int
	var once Once
	once.Do(func() { calls++ })
	once.Do(func() { calls++ })
	once.Do(func() { calls++ })
	require.Equal(t, 1, calls)

	once.Reset()
	once.Do(func() { calls++ })
	once.Do(func() { calls++ })
	once.Do(func() { calls++ })
	require.Equal(t, 2, calls)
}
