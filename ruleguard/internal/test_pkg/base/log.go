package base

import (
	"context"
	"fmt"
	"log"
)

func PanicfCtx(ctx context.Context, format string, args ...interface{}) {
	log.Printf("[ERR] "+format, args...)
}

func InfofCtx(ctx context.Context, logkey, format string, args ...interface{}) {
	format = fmt.Sprintf("[INF] %s: %s", logkey, format)
	log.Printf(format, args...)
}
