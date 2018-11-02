package base

import (
	"testing"
)

type TestState struct {
	s string
}

func (t TestState) String() string {
	return t.s
}

type TestTransition struct {
	s string
}

func (t TestTransition) String() string {
	return t.s
}


func TestStateMachine(t *testing.T) {

	// States
	notStarted := TestState{"NotStarted"}
	running := TestState{"Running"}
	finished := TestState{"Finished"}

	// Transitions
	start := TestTransition{"Start"}
	stop := TestTransition{"Stop"}

	// Init state machine with states and transitions
	stateMachine := NewStateMachine(notStarted)
	stateMachine.AddState(running)
	stateMachine.AddState(finished)
	stateMachine.AddTransition(start, notStarted, running)
	stateMachine.AddTransition(stop, running, finished)

	// Verify initial state
	AssertInState(t, stateMachine, notStarted)

	// Call start transition and validate in takes transition to running state
	if err := stateMachine.DoTransition(start); err != nil {
		t.Fatalf("Got unexpected error")
	}
	AssertInState(t, stateMachine, running)
	AssertNotInState(t, stateMachine, notStarted)
	AssertNotInState(t, stateMachine, finished)

	// Call invalid transition and make sure error is returned and it's in the same state
	if err := stateMachine.DoTransition(start); err == nil {
		t.Fatalf("Expected an error")
	}
	AssertInState(t, stateMachine, running)
	AssertNotInState(t, stateMachine, finished)


}


// 2000000	       802 ns/op
func BenchmarkStateMachine(b *testing.B) {

	// States
	notStarted := TestState{"NotStarted"}
	running := TestState{"Running"}
	finished := TestState{"Finished"}

	// Transitions
	start := TestTransition{"Start"}
	stop := TestTransition{"Stop"}
	reset := TestTransition{"Reset"}

	// Init state machine with states and transitions
	stateMachine := NewStateMachine(notStarted)
	stateMachine.AddState(running)
	stateMachine.AddState(finished)
	stateMachine.AddTransition(start, notStarted, running)
	stateMachine.AddTransition(stop, running, finished)
	stateMachine.AddTransition(reset, finished, notStarted)


	b.ResetTimer()

	for n := 0; n < b.N; n++ {

		stateMachine.InState(notStarted)
		stateMachine.DoTransition(start)
		stateMachine.InState(running)
		stateMachine.DoTransition(stop)
		stateMachine.InState(finished)
		stateMachine.DoTransition(reset)

	}
}


func AssertInState(t *testing.T, stateMachine *StateMachine, state TestState) {
	if !stateMachine.InState(state) {
		t.Fatalf("Expected state machine to be in state: %v", state)
	}
}

func AssertNotInState(t *testing.T, stateMachine *StateMachine, state TestState) {
	if stateMachine.InState(state) {
		t.Fatalf("Expected state machine to NOT be in state: %v", state)
	}
}



