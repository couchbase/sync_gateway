package base

import (
	"fmt"
	"sync"
)


// State Machine
type StateMachine struct {

	// The current state.  Should never be nil as it takes the initial state
	// in the NewStateMachine
	currentState *stateNode

	stateGraph map[stateNode][]transitionEdge

	mutex sync.Mutex

}

type State interface {
	fmt.Stringer
}

type Transition interface {
	fmt.Stringer
}

func NewStateMachine(initialState State) *StateMachine {
	sm := &StateMachine{
		stateGraph: map[stateNode][]transitionEdge{},
	}
	sm.AddState(initialState)
	sm.setInitialState(initialState)
	return sm
}

func (sm *StateMachine) AddState(state State) error {

	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	node := stateNode{
		state: state,
	}
	_, found := sm.stateGraph[node]
	if found {
		return fmt.Errorf("State %v already added", state)
	}

	sm.stateGraph[node] = []transitionEdge{}

	return nil
}


func (sm *StateMachine) AddTransition(transition Transition, fromState State, toState State) error {

	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	fromStateNode := stateNode{
		state: fromState,
	}
	transitionEdges, found := sm.stateGraph[fromStateNode]
	if !found {
		return fmt.Errorf("Could not find fromState: %v", fromState)
	}

	toStateNode := stateNode{
		state: toState,
	}
	_, found = sm.stateGraph[toStateNode]
	if !found {
		return fmt.Errorf("Could not find toStateNode: %v", toStateNode)
	}

	newTransitionEdge := transitionEdge{
		transition: transition,
		fromState:  fromStateNode,
		toState:    toStateNode,
	}
	transitionEdges = append(transitionEdges, newTransitionEdge)
	sm.stateGraph[fromStateNode] = transitionEdges

	return nil
}

func (sm *StateMachine) DoTransition(transition Transition) error {

	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	// Is this a valid transition out of this state?  If not, return
	fromStateNode := stateNode{
		state: sm.currentState.state,
	}
	transitionEdges, found := sm.stateGraph[fromStateNode]
	if !found {
		return fmt.Errorf("Could not find fromState: %v", *sm.currentState)
	}

	foundMatchingTransition := false
	for _, transitionEdge := range transitionEdges {
		if transitionEdge.transition != transition {
			continue
		}

		// If we got this far, found a valid transition
		foundMatchingTransition = true

		// Update the current state to the transition toState
		sm.currentState = &transitionEdge.toState

	}

	if !foundMatchingTransition {
		return fmt.Errorf("Did not expect transition %v from state: %v", transition, *sm.currentState)
	}


	return nil

}


func (sm *StateMachine) InState(state State) (inState bool, err error) {

	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	if sm.currentState == nil {
		return false, fmt.Errorf("Invalid current state (nil)")
	}

	return sm.currentState.state == state, nil

}


func (sm *StateMachine) setInitialState(state State) error {

	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	if sm.currentState != nil {
		return fmt.Errorf("SetInitialState already called.  Current state: %v", *sm.currentState)
	}

	curStateNode := stateNode{
		state: state,
	}

	sm.currentState = &curStateNode
	return nil
}

type stateNode struct {
	state State
}

type transitionEdge struct {
	fromState  stateNode
	toState    stateNode
	transition Transition
}
