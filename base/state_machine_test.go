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

	notStarted := TestState{"NotStarted"}
	running := TestState{"Running"}
	finished := TestState{"Finished"}

	start := TestTransition{"Start"}
	stop := TestTransition{"Stop"}

	stateMachine := NewStateMachine(notStarted)
	stateMachine.AddState(running)
	stateMachine.AddState(finished)
	stateMachine.AddTransition(start, notStarted, running)
	stateMachine.AddTransition(stop, running, finished)

	// Verify initial state
	inState, err := stateMachine.InState(notStarted)
	if err != nil {
		t.Fatalf("Got unexpected error")
	}

	// Call start transition and validate in takes transition to running state
	if err := stateMachine.DoTransition(start); err != nil {
		t.Fatalf("Got unexpected error")
	}
	inState, err = stateMachine.InState(running)
	if err != nil {
		t.Fatalf("Got unexpected error")
	}
	if !inState {
		t.Fatalf("Expected to be in the running state")
	}

	inState, err = stateMachine.InState(notStarted)
	if inState {
		t.Fatalf("Expected to NOT be in the notStarted state")
	}
	if err != nil {
		t.Fatalf("Got unexpected error")
	}

	inState, err = stateMachine.InState(finished)
	if inState {
		t.Fatalf("Expected to NOT be in the finished state")
	}
	if err != nil {
		t.Fatalf("Got unexpected error")
	}

	// Call invalid transition and make sure error is returned and it's in the same state
	if err := stateMachine.DoTransition(start); err == nil {
		t.Fatalf("Expected an error")
	}
	inState, err = stateMachine.InState(running)
	if err != nil {
		t.Fatalf("Got unexpected error")
	}
	if !inState {
		t.Fatalf("Expected to be in the running state")
	}


}
