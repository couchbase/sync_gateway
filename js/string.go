package js

// A string type that v8Runner.NewValue will treat specially, by parsing it as JSON and converting
// it to a JavaScript object.
type JSONString string
