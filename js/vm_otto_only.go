package js

// Returns the Engine with the given name, else nil.
// Valid names are "V8" and "Otto", which map to the instances `V8` and `Otto`.
func EngineNamed(name string) *Engine {
	switch name {
	case ottoVMName:
		return Otto
	default:
		return nil
	}
}
