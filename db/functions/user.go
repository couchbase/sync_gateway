package functions

// Name and capabilities of the current user. (Admin is represented by a nil `*userCredentials`.)
type userCredentials struct {
	Name     string
	Roles    []string
	Channels []string
}
