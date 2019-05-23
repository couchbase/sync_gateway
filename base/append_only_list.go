package base

// AppendOnlyList is a doubly-linked list that only supports adding elements to the list
// via PushBack.  Based on standard library list, but omits length property to allow for
// concurrent append and snapshot iteration

// AppendOnlyElement is an element of an AppendOnlyList.
type AppendOnlyElement struct {
	// Next and previous pointers in the doubly-linked list of elements.
	// To simplify the implementation, internally a list l is implemented
	// as a ring, such that &l.root is both the next element of the last
	// list element (l.Back()) and the previous element of the first list
	// element (l.Front()).
	next, prev *AppendOnlyElement

	// The list to which this element belongs.
	list *AppendOnlyList

	// The value stored with this element.
	Value interface{}
}

// Next returns the next list element or nil.
func (e *AppendOnlyElement) Next() *AppendOnlyElement {
	if p := e.next; e.list != nil && p != &e.list.root {
		return p
	}
	return nil
}

// Prev returns the previous list element or nil.
func (e *AppendOnlyElement) Prev() *AppendOnlyElement {
	if p := e.prev; e.list != nil && p != &e.list.root {
		return p
	}
	return nil
}

// List represents a doubly linked list.
// The zero value for List is an empty list ready to use.
type AppendOnlyList struct {
	root AppendOnlyElement // sentinel list element, only &root, root.prev, and root.next are used
}

// Init initializes or clears list l.
func (l *AppendOnlyList) Init() *AppendOnlyList {
	l.root.next = &l.root
	l.root.prev = &l.root
	return l
}

// New returns an initialized list.
func NewAppendOnlyList() *AppendOnlyList { return new(AppendOnlyList).Init() }

// Front returns the first element of list l or nil if the list is empty.
func (l *AppendOnlyList) Front() *AppendOnlyElement {
	if l.root.next == &l.root {
		return nil
	}
	return l.root.next
}

// Back returns the last element of list l or nil if the list is empty.
func (l *AppendOnlyList) Back() *AppendOnlyElement {
	if l.root.prev == &l.root {
		return nil
	}
	return l.root.prev
}

// Since AppendOnlyList doesn't maintain length internally, Len requires iteration
// over the list and is O(n)
func (l *AppendOnlyList) Len() int {
	len := 0
	for e := l.Front(); e != nil; e = e.Next() {
		len++
	}
	return len
}

// lazyInit lazily initializes a zero List value.
func (l *AppendOnlyList) lazyInit() {
	if l.root.next == nil {
		l.Init()
	}
}

// insert inserts e after at, increments l.len, and returns e.
func (l *AppendOnlyList) insert(e, at *AppendOnlyElement) *AppendOnlyElement {
	n := at.next
	at.next = e
	e.prev = at
	e.next = n
	n.prev = e
	e.list = l
	return e
}

// insertValue is a convenience wrapper for insert(&Element{Value: v}, at).
func (l *AppendOnlyList) insertValue(v interface{}, at *AppendOnlyElement) *AppendOnlyElement {
	return l.insert(&AppendOnlyElement{Value: v}, at)
}

// remove removes e from its list, decrements l.len, and returns e.
func (l *AppendOnlyList) remove(e *AppendOnlyElement) *AppendOnlyElement {
	e.prev.next = e.next
	e.next.prev = e.prev
	e.next = nil // avoid memory leaks
	e.prev = nil // avoid memory leaks
	e.list = nil
	return e
}

// Remove removes e from l if e is an element of list l.
// It returns the element value e.Value.
// The element must not be nil.
func (l *AppendOnlyList) Remove(e *AppendOnlyElement) interface{} {
	if e.list == l {
		// if e.list == l, l must have been initialized when e was inserted
		// in l or l == nil (e is a zero Element) and l.remove will crash
		l.remove(e)
	}
	return e.Value
}

// PushBack inserts a new element e with value v at the back of list l and returns e.
func (l *AppendOnlyList) PushBack(v interface{}) *AppendOnlyElement {
	l.lazyInit()
	return l.insertValue(v, l.root.prev)
}
