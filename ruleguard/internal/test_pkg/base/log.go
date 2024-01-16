package base

import (
	"context"
	"fmt"
	"log"
)

//nolint:goprintffuncname
func PanicfCtx(_ context.Context, format string, args ...interface{}) {
	log.Printf("[ERR] "+format, args...)
}

//nolint:goprintffuncname
func InfofCtx(_ context.Context, logkey, format string, args ...interface{}) {
	format = fmt.Sprintf("[INF] %s: %s", logkey, format)
	log.Printf(format, args...)
}
