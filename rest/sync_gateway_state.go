package rest

// States
var (
	NotStarted = SGState{"NotStarted"}
	Running = SGState{"Running"}
	Finished = SGState{"Finished"}
)

// Transitions
var (
	Start = SGTransition{"Start"}
	Stop = SGTransition{"Stop"}
)

type SGState struct {
	name string
}

func (s SGState) String() string {
	return s.name
}

type SGTransition struct {
	name string
}

func (s SGTransition) String() string {
	return s.name
}

