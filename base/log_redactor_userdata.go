package base

// UserData implements the Redactor interface for logging purposes.
type UserData string

// Compile-time interface check.
var _ Redactor = UserData("")

const (
	userDataPrefix = "<ud>"
	userDataSuffix = "</ud>"
)

func (ud UserData) Redact() string {
	return userDataPrefix + string(ud) + userDataSuffix
}
