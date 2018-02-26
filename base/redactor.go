package base

// Redactor provides an interface for log redaction.
type Redactor interface {
	// Redact returns the given string in a redacted form. This may be tagged,
	// changed, hashed, or removed completely depending on desired behaviour.
	Redact() string
}

// Redact performs an *in-place* redaction on the input slice, and returns it.
// This should only be consumed by logging funcs. E.g. fmt.Printf(fmt, Redact(args))
//
// FIXME: Ideally this would be private, but we'd need to pull 'base/logging.go' into this package.
func Redact(args []interface{}) []interface{} {
	for i, v := range args {
		if r, ok := v.(Redactor); ok {
			args[i] = r.Redact()
		}
	}
	return args
}
