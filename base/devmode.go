package base

// IsDevMode returns true when compiled with the `cb_sg_devmode` build tag, and false otherwise.
//
// The compiler will remove this check and all code invoked inside it in non-dev mode, avoiding any impact on production code.
// https://godbolt.org/z/f1K8a96rE
func IsDevMode() bool {
	return cbSGDevModeBuildTagSet
}
