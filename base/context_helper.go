package base

import "context"

// Helper functions/structs associated with context.Context()

// Associate a CancelFunc with it's Context
type CancellableContext struct {
	CancelFunc context.CancelFunc
	Context context.Context
}