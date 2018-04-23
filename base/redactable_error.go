package base

import "fmt"

// A redactable error can be used as a drop-in replacement for a base error (as would have been created via
// fmt.Errorf), which has the ability to redact any sensitive user data by calling redact() on all if it's
// stored args.
type RedactableError struct {
	fmt string
	args []interface{}
}

// Create a new redactable error.  Same signature as fmt.Errorf() for easy drop-in replacement.
func RedactErrorf(fmt string, args ...interface{}) *RedactableError {
	return &RedactableError{
		fmt: fmt,
		args: args,
	}
}

// Satisfy error interface
func (re *RedactableError) Error() string {
	return fmt.Sprintf(re.fmt, re.args...)
}

// Satisfy redact interface
func (re *RedactableError) Redact() string {
	redactedArgs := redact(re.args)
	return fmt.Sprintf(re.fmt, redactedArgs...)
}