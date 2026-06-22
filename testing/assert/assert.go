// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package assert

import (
	"net/http"
	"net/url"
	"time"

	"github.com/stretchr/testify/assert"
)

type TestingT = assert.TestingT
type CollectT = assert.CollectT
type Comparison = assert.Comparison
type PanicTestFunc = assert.PanicTestFunc
type ErrorAssertionFunc = assert.ErrorAssertionFunc
type ValueAssertionFunc = assert.ValueAssertionFunc
type ComparisonAssertionFunc = assert.ComparisonAssertionFunc
type BoolAssertionFunc = assert.BoolAssertionFunc

// Generic wrapper functions for functions taking two interface{} arguments:

// Contains asserts that the specified string, list(array, slice...) or map
// contains the specified substring or element.
//
//	assert.Contains(t, "Hello World", "World")
//	assert.Contains(t, ["Hello", "World"], "World")
//	assert.Contains(t, {"Hello": "World"}, "Hello")
func Contains[T1, T2 any](t TestingT, s T1, contains T2, msgAndArgs ...any) bool {
	return assert.Contains(t, s, contains, msgAndArgs...)
}

// Containsf asserts that the specified string, list(array, slice...) or map
// contains the specified substring or element.
//
//	assert.Containsf(t, "Hello World", "World", "error message %s", "formatted")
//	assert.Containsf(t, ["Hello", "World"], "World", "error message %s", "formatted")
//	assert.Containsf(t, {"Hello": "World"}, "Hello", "error message %s", "formatted")
func Containsf[T1, T2 any](t TestingT, s T1, contains T2, msg string, args ...any) bool {
	return assert.Containsf(t, s, contains, msg, args...)
}

// ElementsMatch asserts that the specified listA(array, slice...) is equal
// to specified listB(array, slice...) ignoring the order of the elements.
// If there are duplicate elements, the number of appearances of each of them
// in both lists should match.
//
// assert.ElementsMatch(t, [1, 3, 2, 3], [1, 3, 3, 2])
func ElementsMatch[T1, T2 any](t TestingT, listA T1, listB T2, msgAndArgs ...any) bool {
	return assert.ElementsMatch(t, listA, listB, msgAndArgs...)
}

// ElementsMatchf asserts that the specified listA(array, slice...) is equal
// to specified listB(array, slice...) ignoring the order of the elements.
// If there are duplicate elements, the number of appearances of each of them
// in both lists should match.
//
// assert.ElementsMatchf(t, [1, 3, 2, 3], [1, 3, 3, 2], "error message %s",
// "formatted")
func ElementsMatchf[T1, T2 any](t TestingT, listA T1, listB T2, msg string, args ...any) bool {
	return assert.ElementsMatchf(t, listA, listB, msg, args...)
}

// Equal asserts that two objects are equal.
//
//	assert.Equal(t, 123, 123)
//
// Pointer variable equality is determined based on the equality of the
// referenced values (as opposed to the memory addresses). Function equality
// cannot be determined and will always fail.
func Equal[T1, T2 any](t TestingT, expected T1, actual T2, msgAndArgs ...any) bool {
	return assert.Equal(t, expected, actual, msgAndArgs...)
}

// Equalf asserts that two objects are equal.
//
//	assert.Equalf(t, 123, 123, "error message %s", "formatted")
//
// Pointer variable equality is determined based on the equality of the
// referenced values (as opposed to the memory addresses). Function equality
// cannot be determined and will always fail.
func Equalf[T1, T2 any](t TestingT, expected T1, actual T2, msg string, args ...any) bool {
	return assert.Equalf(t, expected, actual, msg, args...)
}

// EqualExportedValues asserts that the types of two objects are equal and
// their public fields are also equal. This is useful for comparing structs
// that have private fields that could potentially differ.
//
//	 type S struct {
//		Exported     	int
//		notExported   	int
//	 }
//	 assert.EqualExportedValues(t, S{1, 2}, S{1, 3}) => true
//	 assert.EqualExportedValues(t, S{1, 2}, S{2, 3}) => false
func EqualExportedValues[T1, T2 any](t TestingT, expected T1, actual T2, msgAndArgs ...any) bool {
	return assert.EqualExportedValues(t, expected, actual, msgAndArgs...)
}

// EqualExportedValuesf asserts that the types of two objects are equal and
// their public fields are also equal. This is useful for comparing structs
// that have private fields that could potentially differ.
//
//	 type S struct {
//		Exported     	int
//		notExported   	int
//	 }
//	 assert.EqualExportedValuesf(t, S{1, 2}, S{1, 3}, "error message %s", "formatted") => true
//	 assert.EqualExportedValuesf(t, S{1, 2}, S{2, 3}, "error message %s", "formatted") => false
func EqualExportedValuesf[T1, T2 any](t TestingT, expected T1, actual T2, msg string, args ...any) bool {
	return assert.EqualExportedValuesf(t, expected, actual, msg, args...)
}

// EqualValues asserts that two objects are equal or convertible to the larger
// type and equal.
//
//	assert.EqualValues(t, uint32(123), int32(123))
func EqualValues[T1, T2 any](t TestingT, expected T1, actual T2, msgAndArgs ...any) bool {
	return assert.EqualValues(t, expected, actual, msgAndArgs...)
}

// EqualValuesf asserts that two objects are equal or convertible to the larger
// type and equal.
//
//	assert.EqualValuesf(t, uint32(123), int32(123), "error message %s", "formatted")
func EqualValuesf[T1, T2 any](t TestingT, expected T1, actual T2, msg string, args ...any) bool {
	return assert.EqualValuesf(t, expected, actual, msg, args...)
}

// Exactly asserts that two objects are equal in value and type.
//
//	assert.Exactly(t, int32(123), int64(123))
func Exactly[T1, T2 any](t TestingT, expected T1, actual T2, msgAndArgs ...any) bool {
	return assert.Exactly(t, expected, actual, msgAndArgs...)
}

// Exactlyf asserts that two objects are equal in value and type.
//
//	assert.Exactlyf(t, int32(123), int64(123), "error message %s", "formatted")
func Exactlyf[T1, T2 any](t TestingT, expected T1, actual T2, msg string, args ...any) bool {
	return assert.Exactlyf(t, expected, actual, msg, args...)
}

// Greater asserts that the first element is greater than the second
//
//	assert.Greater(t, 2, 1)
//	assert.Greater(t, float64(2), float64(1))
//	assert.Greater(t, "b", "a")
func Greater[T1, T2 any](t TestingT, e1 T1, e2 T2, msgAndArgs ...any) bool {
	return assert.Greater(t, e1, e2, msgAndArgs...)
}

// Greaterf asserts that the first element is greater than the second
//
//	assert.Greaterf(t, 2, 1, "error message %s", "formatted")
//	assert.Greaterf(t, float64(2), float64(1), "error message %s", "formatted")
//	assert.Greaterf(t, "b", "a", "error message %s", "formatted")
func Greaterf[T1, T2 any](t TestingT, e1 T1, e2 T2, msg string, args ...any) bool {
	return assert.Greaterf(t, e1, e2, msg, args...)
}

// GreaterOrEqual asserts that the first element is greater than or equal to
// the second
//
//	assert.GreaterOrEqual(t, 2, 1)
//	assert.GreaterOrEqual(t, 2, 2)
//	assert.GreaterOrEqual(t, "b", "a")
//	assert.GreaterOrEqual(t, "b", "b")
func GreaterOrEqual[T1, T2 any](t TestingT, e1 T1, e2 T2, msgAndArgs ...any) bool {
	return assert.GreaterOrEqual(t, e1, e2, msgAndArgs...)
}

// GreaterOrEqualf asserts that the first element is greater than or equal to
// the second
//
//	assert.GreaterOrEqualf(t, 2, 1, "error message %s", "formatted")
//	assert.GreaterOrEqualf(t, 2, 2, "error message %s", "formatted")
//	assert.GreaterOrEqualf(t, "b", "a", "error message %s", "formatted")
//	assert.GreaterOrEqualf(t, "b", "b", "error message %s", "formatted")
func GreaterOrEqualf[T1, T2 any](t TestingT, e1 T1, e2 T2, msg string, args ...any) bool {
	return assert.GreaterOrEqualf(t, e1, e2, msg, args...)
}

// IsNotType asserts that the specified objects are not of the same type.
//
//	assert.IsNotType(t, &NotMyStruct{}, &MyStruct{})
func IsNotType[T1, T2 any](t TestingT, theType T1, object T2, msgAndArgs ...any) bool {
	return assert.IsNotType(t, theType, object, msgAndArgs...)
}

// IsNotTypef asserts that the specified objects are not of the same type.
//
//	assert.IsNotTypef(t, &NotMyStruct{}, &MyStruct{}, "error message %s", "formatted")
func IsNotTypef[T1, T2 any](t TestingT, theType T1, object T2, msg string, args ...any) bool {
	return assert.IsNotTypef(t, theType, object, msg, args...)
}

// IsType asserts that the specified objects are of the same type.
//
//	assert.IsType(t, &MyStruct{}, &MyStruct{})
func IsType[T1, T2 any](t TestingT, expectedType T1, object T2, msgAndArgs ...any) bool {
	return assert.IsType(t, expectedType, object, msgAndArgs...)
}

// IsTypef asserts that the specified objects are of the same type.
//
//	assert.IsTypef(t, &MyStruct{}, &MyStruct{}, "error message %s", "formatted")
func IsTypef[T1, T2 any](t TestingT, expectedType T1, object T2, msg string, args ...any) bool {
	return assert.IsTypef(t, expectedType, object, msg, args...)
}

// Less asserts that the first element is less than the second
//
//	assert.Less(t, 1, 2)
//	assert.Less(t, float64(1), float64(2))
//	assert.Less(t, "a", "b")
func Less[T1, T2 any](t TestingT, e1 T1, e2 T2, msgAndArgs ...any) bool {
	return assert.Less(t, e1, e2, msgAndArgs...)
}

// Lessf asserts that the first element is less than the second
//
//	assert.Lessf(t, 1, 2, "error message %s", "formatted")
//	assert.Lessf(t, float64(1), float64(2), "error message %s", "formatted")
//	assert.Lessf(t, "a", "b", "error message %s", "formatted")
func Lessf[T1, T2 any](t TestingT, e1 T1, e2 T2, msg string, args ...any) bool {
	return assert.Lessf(t, e1, e2, msg, args...)
}

// LessOrEqual asserts that the first element is less than or equal to the
// second
//
//	assert.LessOrEqual(t, 1, 2)
//	assert.LessOrEqual(t, 2, 2)
//	assert.LessOrEqual(t, "a", "b")
//	assert.LessOrEqual(t, "b", "b")
func LessOrEqual[T1, T2 any](t TestingT, e1 T1, e2 T2, msgAndArgs ...any) bool {
	return assert.LessOrEqual(t, e1, e2, msgAndArgs...)
}

// LessOrEqualf asserts that the first element is less than or equal to the
// second
//
//	assert.LessOrEqualf(t, 1, 2, "error message %s", "formatted")
//	assert.LessOrEqualf(t, 2, 2, "error message %s", "formatted")
//	assert.LessOrEqualf(t, "a", "b", "error message %s", "formatted")
//	assert.LessOrEqualf(t, "b", "b", "error message %s", "formatted")
func LessOrEqualf[T1, T2 any](t TestingT, e1 T1, e2 T2, msg string, args ...any) bool {
	return assert.LessOrEqualf(t, e1, e2, msg, args...)
}

// NotContains asserts that the specified string, list(array, slice...) or map
// does NOT contain the specified substring or element.
//
//	assert.NotContains(t, "Hello World", "Earth")
//	assert.NotContains(t, ["Hello", "World"], "Earth")
//	assert.NotContains(t, {"Hello": "World"}, "Earth")
func NotContains[T1, T2 any](t TestingT, s T1, contains T2, msgAndArgs ...any) bool {
	return assert.NotContains(t, s, contains, msgAndArgs...)
}

// NotContainsf asserts that the specified string, list(array, slice...) or map
// does NOT contain the specified substring or element.
//
//	assert.NotContainsf(t, "Hello World", "Earth", "error message %s", "formatted")
//	assert.NotContainsf(t, ["Hello", "World"], "Earth", "error message %s", "formatted")
//	assert.NotContainsf(t, {"Hello": "World"}, "Earth", "error message %s", "formatted")
func NotContainsf[T1, T2 any](t TestingT, s T1, contains T2, msg string, args ...any) bool {
	return assert.NotContainsf(t, s, contains, msg, args...)
}

// NotElementsMatch asserts that the specified listA(array, slice...) is
// NOT equal to specified listB(array, slice...) ignoring the order of the
// elements. If there are duplicate elements, the number of appearances of each
// of them in both lists should not match. This is an inverse of ElementsMatch.
//
// assert.NotElementsMatch(t, [1, 1, 2, 3], [1, 1, 2, 3]) -> false
//
// assert.NotElementsMatch(t, [1, 1, 2, 3], [1, 2, 3]) -> true
//
// assert.NotElementsMatch(t, [1, 2, 3], [1, 2, 4]) -> true
func NotElementsMatch[T1, T2 any](t TestingT, listA T1, listB T2, msgAndArgs ...any) bool {
	return assert.NotElementsMatch(t, listA, listB, msgAndArgs...)
}

// NotElementsMatchf asserts that the specified listA(array, slice...) is
// NOT equal to specified listB(array, slice...) ignoring the order of the
// elements. If there are duplicate elements, the number of appearances of each
// of them in both lists should not match. This is an inverse of ElementsMatch.
//
// assert.NotElementsMatchf(t, [1, 1, 2, 3], [1, 1, 2, 3], "error message %s",
// "formatted") -> false
//
// assert.NotElementsMatchf(t, [1, 1, 2, 3], [1, 2, 3], "error message %s",
// "formatted") -> true
//
// assert.NotElementsMatchf(t, [1, 2, 3], [1, 2, 4], "error message %s",
// "formatted") -> true
func NotElementsMatchf[T1, T2 any](t TestingT, listA T1, listB T2, msg string, args ...any) bool {
	return assert.NotElementsMatchf(t, listA, listB, msg, args...)
}

// NotEqual asserts that the specified values are NOT equal.
//
//	assert.NotEqual(t, obj1, obj2)
//
// Pointer variable equality is determined based on the equality of the
// referenced values (as opposed to the memory addresses).
func NotEqual[T1, T2 any](t TestingT, expected T1, actual T2, msgAndArgs ...any) bool {
	return assert.NotEqual(t, expected, actual, msgAndArgs...)
}

// NotEqualValues asserts that two objects are not equal even when converted to
// the same type
//
//	assert.NotEqualValues(t, obj1, obj2)
func NotEqualValues[T1, T2 any](t TestingT, expected T1, actual T2, msgAndArgs ...any) bool {
	return assert.NotEqualValues(t, expected, actual, msgAndArgs...)
}

// NotEqualValuesf asserts that two objects are not equal even when converted
// to the same type
//
//	assert.NotEqualValuesf(t, obj1, obj2, "error message %s", "formatted")
func NotEqualValuesf[T1, T2 any](t TestingT, expected T1, actual T2, msg string, args ...any) bool {
	return assert.NotEqualValuesf(t, expected, actual, msg, args...)
}

// NotEqualf asserts that the specified values are NOT equal.
//
//	assert.NotEqualf(t, obj1, obj2, "error message %s", "formatted")
//
// Pointer variable equality is determined based on the equality of the
// referenced values (as opposed to the memory addresses).
func NotEqualf[T1, T2 any](t TestingT, expected T1, actual T2, msg string, args ...any) bool {
	return assert.NotEqualf(t, expected, actual, msg, args...)
}

// NotImplements asserts that an object does not implement the specified
// interface.
//
//	assert.NotImplements(t, (*MyInterface)(nil), new(MyObject))
func NotImplements[T1, T2 any](t TestingT, interfaceObject T1, object T2, msgAndArgs ...any) bool {
	return assert.NotImplements(t, interfaceObject, object, msgAndArgs...)
}

// NotImplementsf asserts that an object does not implement the specified
// interface.
//
//	assert.NotImplementsf(t, (*MyInterface)(nil), new(MyObject), "error message %s", "formatted")
func NotImplementsf[T1, T2 any](t TestingT, interfaceObject T1, object T2, msg string, args ...any) bool {
	return assert.NotImplementsf(t, interfaceObject, object, msg, args...)
}

// NotRegexp asserts that a specified regexp does not match a string.
//
//	assert.NotRegexp(t, regexp.MustCompile("starts"), "it's starting")
//	assert.NotRegexp(t, "^start", "it's not starting")
func NotRegexp[T1, T2 any](t TestingT, rx T1, str T2, msgAndArgs ...any) bool {
	return assert.NotRegexp(t, rx, str, msgAndArgs...)
}

// NotRegexpf asserts that a specified regexp does not match a string.
//
//	assert.NotRegexpf(t, regexp.MustCompile("starts"), "it's starting", "error message %s", "formatted")
//	assert.NotRegexpf(t, "^start", "it's not starting", "error message %s", "formatted")
func NotRegexpf[T1, T2 any](t TestingT, rx T1, str T2, msg string, args ...any) bool {
	return assert.NotRegexpf(t, rx, str, msg, args...)
}

// NotSame asserts that two pointers do not reference the same object.
//
//	assert.NotSame(t, ptr1, ptr2)
//
// Both arguments must be pointer variables. Pointer variable sameness is
// determined based on the equality of both type and value.
func NotSame[T1, T2 any](t TestingT, expected T1, actual T2, msgAndArgs ...any) bool {
	return assert.NotSame(t, expected, actual, msgAndArgs...)
}

// NotSamef asserts that two pointers do not reference the same object.
//
//	assert.NotSamef(t, ptr1, ptr2, "error message %s", "formatted")
//
// Both arguments must be pointer variables. Pointer variable sameness is
// determined based on the equality of both type and value.
func NotSamef[T1, T2 any](t TestingT, expected T1, actual T2, msg string, args ...any) bool {
	return assert.NotSamef(t, expected, actual, msg, args...)
}

// NotSubset asserts that the list (array, slice, or map) does NOT contain
// all elements given in the subset (array, slice, or map). Map elements are
// key-value pairs unless compared with an array or slice where only the map
// key is evaluated.
//
//	assert.NotSubset(t, [1, 3, 4], [1, 2])
//	assert.NotSubset(t, {"x": 1, "y": 2}, {"z": 3})
//	assert.NotSubset(t, [1, 3, 4], {1: "one", 2: "two"})
//	assert.NotSubset(t, {"x": 1, "y": 2}, ["z"])
func NotSubset[T1, T2 any](t TestingT, list T1, subset T2, msgAndArgs ...any) bool {
	return assert.NotSubset(t, list, subset, msgAndArgs...)
}

// NotSubsetf asserts that the list (array, slice, or map) does NOT contain
// all elements given in the subset (array, slice, or map). Map elements are
// key-value pairs unless compared with an array or slice where only the map
// key is evaluated.
//
//	assert.NotSubsetf(t, [1, 3, 4], [1, 2], "error message %s", "formatted")
//	assert.NotSubsetf(t, {"x": 1, "y": 2}, {"z": 3}, "error message %s", "formatted")
//	assert.NotSubsetf(t, [1, 3, 4], {1: "one", 2: "two"}, "error message %s", "formatted")
//	assert.NotSubsetf(t, {"x": 1, "y": 2}, ["z"], "error message %s", "formatted")
func NotSubsetf[T1, T2 any](t TestingT, list T1, subset T2, msg string, args ...any) bool {
	return assert.NotSubsetf(t, list, subset, msg, args...)
}

// Regexp asserts that a specified regexp matches a string.
//
//	assert.Regexp(t, regexp.MustCompile("start"), "it's starting")
//	assert.Regexp(t, "start...$", "it's not starting")
func Regexp[T1, T2 any](t TestingT, rx T1, str T2, msgAndArgs ...any) bool {
	return assert.Regexp(t, rx, str, msgAndArgs...)
}

// Regexpf asserts that a specified regexp matches a string.
//
//	assert.Regexpf(t, regexp.MustCompile("start"), "it's starting", "error message %s", "formatted")
//	assert.Regexpf(t, "start...$", "it's not starting", "error message %s", "formatted")
func Regexpf[T1, T2 any](t TestingT, rx T1, str T2, msg string, args ...any) bool {
	return assert.Regexpf(t, rx, str, msg, args...)
}

// Same asserts that two pointers reference the same object.
//
//	assert.Same(t, ptr1, ptr2)
//
// Both arguments must be pointer variables. Pointer variable sameness is
// determined based on the equality of both type and value.
func Same[T1, T2 any](t TestingT, expected T1, actual T2, msgAndArgs ...any) bool {
	return assert.Same(t, expected, actual, msgAndArgs...)
}

// Samef asserts that two pointers reference the same object.
//
//	assert.Samef(t, ptr1, ptr2, "error message %s", "formatted")
//
// Both arguments must be pointer variables. Pointer variable sameness is
// determined based on the equality of both type and value.
func Samef[T1, T2 any](t TestingT, expected T1, actual T2, msg string, args ...any) bool {
	return assert.Samef(t, expected, actual, msg, args...)
}

// Subset asserts that the list (array, slice, or map) contains all elements
// given in the subset (array, slice, or map). Map elements are key-value pairs
// unless compared with an array or slice where only the map key is evaluated.
//
//	assert.Subset(t, [1, 2, 3], [1, 2])
//	assert.Subset(t, {"x": 1, "y": 2}, {"x": 1})
//	assert.Subset(t, [1, 2, 3], {1: "one", 2: "two"})
//	assert.Subset(t, {"x": 1, "y": 2}, ["x"])
func Subset[T1, T2 any](t TestingT, list T1, subset T2, msgAndArgs ...any) bool {
	return assert.Subset(t, list, subset, msgAndArgs...)
}

// Subsetf asserts that the list (array, slice, or map) contains all elements
// given in the subset (array, slice, or map). Map elements are key-value pairs
// unless compared with an array or slice where only the map key is evaluated.
//
//	assert.Subsetf(t, [1, 2, 3], [1, 2], "error message %s", "formatted")
//	assert.Subsetf(t, {"x": 1, "y": 2}, {"x": 1}, "error message %s", "formatted")
//	assert.Subsetf(t, [1, 2, 3], {1: "one", 2: "two"}, "error message %s", "formatted")
//	assert.Subsetf(t, {"x": 1, "y": 2}, ["x"], "error message %s", "formatted")
func Subsetf[T1, T2 any](t TestingT, list T1, subset T2, msg string, args ...any) bool {
	return assert.Subsetf(t, list, subset, msg, args...)
}

// InDelta asserts that the two numerals are within delta of each other.
//
//	assert.InDelta(t, math.Pi, 22/7.0, 0.01)
func InDelta[T1, T2 any](t TestingT, expected T1, actual T2, delta float64, msgAndArgs ...any) bool {
	return assert.InDelta(t, expected, actual, delta, msgAndArgs...)
}

// InDeltaSlice is the same as InDelta, except it compares two slices.
func InDeltaSlice[T1, T2 any](t TestingT, expected T1, actual T2, delta float64, msgAndArgs ...any) bool {
	return assert.InDeltaSlice(t, expected, actual, delta, msgAndArgs...)
}

// InDeltaSlicef is the same as InDelta, except it compares two slices.
func InDeltaSlicef[T1, T2 any](t TestingT, expected T1, actual T2, delta float64, msg string, args ...any) bool {
	return assert.InDeltaSlicef(t, expected, actual, delta, msg, args...)
}

// InDeltaf asserts that the two numerals are within delta of each other.
//
//	assert.InDeltaf(t, math.Pi, 22/7.0, 0.01, "error message %s", "formatted")
func InDeltaf[T1, T2 any](t TestingT, expected T1, actual T2, delta float64, msg string, args ...any) bool {
	return assert.InDeltaf(t, expected, actual, delta, msg, args...)
}

// InDeltaMapValues is the same as InDelta, but it compares all values between
// two maps. Both maps must have exactly the same keys.
func InDeltaMapValues[T1, T2 any](t TestingT, expected T1, actual T2, delta float64, msgAndArgs ...any) bool {
	return assert.InDeltaMapValues(t, expected, actual, delta, msgAndArgs...)
}

// InDeltaMapValuesf is the same as InDelta, but it compares all values between
// two maps. Both maps must have exactly the same keys.
func InDeltaMapValuesf[T1, T2 any](t TestingT, expected T1, actual T2, delta float64, msg string, args ...any) bool {
	return assert.InDeltaMapValuesf(t, expected, actual, delta, msg, args...)
}

// InEpsilon asserts that expected and actual have a relative error less than
// epsilon
func InEpsilon[T1, T2 any](t TestingT, expected T1, actual T2, epsilon float64, msgAndArgs ...any) bool {
	return assert.InEpsilon(t, expected, actual, epsilon, msgAndArgs...)
}

// InEpsilonSlice is the same as InEpsilon, except it compares each value from
// two slices.
func InEpsilonSlice[T1, T2 any](t TestingT, expected T1, actual T2, epsilon float64, msgAndArgs ...any) bool {
	return assert.InEpsilonSlice(t, expected, actual, epsilon, msgAndArgs...)
}

// InEpsilonSlicef is the same as InEpsilon, except it compares each value from
// two slices.
func InEpsilonSlicef[T1, T2 any](t TestingT, expected T1, actual T2, epsilon float64, msg string, args ...any) bool {
	return assert.InEpsilonSlicef(t, expected, actual, epsilon, msg, args...)
}

// InEpsilonf asserts that expected and actual have a relative error less than
// epsilon
func InEpsilonf[T1, T2 any](t TestingT, expected T1, actual T2, epsilon float64, msg string, args ...any) bool {
	return assert.InEpsilonf(t, expected, actual, epsilon, msg, args...)
}

// Pass-through of all other functions:

// CallerInfo returns an array of strings containing the file and line number
// of each stack frame leading from the current test to the assert call that
// failed.
func CallerInfo() []string {
	return assert.CallerInfo()
}

// Condition uses a Comparison to assert a complex condition.
func Condition(t TestingT, comp Comparison, msgAndArgs ...any) bool {
	return assert.Condition(t, comp, msgAndArgs...)
}

// Conditionf uses a Comparison to assert a complex condition.
func Conditionf(t TestingT, comp Comparison, msg string, args ...any) bool {
	return assert.Conditionf(t, comp, msg, args...)
}

// DirExists checks whether a directory exists in the given path. It also
// fails if the path is a file rather a directory or there is an error checking
// whether it exists.
func DirExists(t TestingT, path string, msgAndArgs ...any) bool {
	return assert.DirExists(t, path, msgAndArgs...)
}

// DirExistsf checks whether a directory exists in the given path. It also
// fails if the path is a file rather a directory or there is an error checking
// whether it exists.
func DirExistsf(t TestingT, path string, msg string, args ...any) bool {
	return assert.DirExistsf(t, path, msg, args...)
}

// Empty asserts that the given value is "empty".
//
// Zero values are "empty".
//
// Arrays are "empty" if every element is the zero value of the type (stricter
// than "empty").
//
// Slices, maps and channels with zero length are "empty".
//
// Pointer values are "empty" if the pointer is nil or if the pointed value is
// "empty".
//
//	assert.Empty(t, obj)
func Empty(t TestingT, object any, msgAndArgs ...any) bool {
	return assert.Empty(t, object, msgAndArgs...)
}

// Emptyf asserts that the given value is "empty".
//
// Zero values are "empty".
//
// Arrays are "empty" if every element is the zero value of the type (stricter
// than "empty").
//
// Slices, maps and channels with zero length are "empty".
//
// Pointer values are "empty" if the pointer is nil or if the pointed value is
// "empty".
//
//	assert.Emptyf(t, obj, "error message %s", "formatted")
func Emptyf(t TestingT, object any, msg string, args ...any) bool {
	return assert.Emptyf(t, object, msg, args...)
}

// EqualError asserts that a function returned an error (i.e. not `nil`) and
// that it is equal to the provided error.
//
//	actualObj, err := SomeFunction()
//	assert.EqualError(t, err,  expectedErrorString)
func EqualError(t TestingT, theError error, errString string, msgAndArgs ...any) bool {
	return assert.EqualError(t, theError, errString, msgAndArgs...)
}

// EqualErrorf asserts that a function returned an error (i.e. not `nil`) and
// that it is equal to the provided error.
//
//	actualObj, err := SomeFunction()
//	assert.EqualErrorf(t, err,  expectedErrorString, "error message %s", "formatted")
func EqualErrorf(t TestingT, theError error, errString string, msg string, args ...any) bool {
	return assert.EqualErrorf(t, theError, errString, msg, args...)
}

// Error asserts that a function returned an error (i.e. not `nil`).
//
//	actualObj, err := SomeFunction()
//	assert.Error(t, err)
func Error(t TestingT, err error, msgAndArgs ...any) bool {
	return assert.Error(t, err, msgAndArgs...)
}

// ErrorAs asserts that at least one of the errors in err's chain matches
// target, and if so, sets target to that error value. This is a wrapper for
// errors.As.
func ErrorAs(t TestingT, err error, target any, msgAndArgs ...any) bool {
	return assert.ErrorAs(t, err, target, msgAndArgs...)
}

// ErrorAsf asserts that at least one of the errors in err's chain matches
// target, and if so, sets target to that error value. This is a wrapper for
// errors.As.
func ErrorAsf(t TestingT, err error, target any, msg string, args ...any) bool {
	return assert.ErrorAsf(t, err, target, msg, args...)
}

// ErrorContains asserts that a function returned an error (i.e. not `nil`) and
// that the error contains the specified substring.
//
//	actualObj, err := SomeFunction()
//	assert.ErrorContains(t, err,  expectedErrorSubString)
func ErrorContains(t TestingT, theError error, contains string, msgAndArgs ...any) bool {
	return assert.ErrorContains(t, theError, contains, msgAndArgs...)
}

// ErrorContainsf asserts that a function returned an error (i.e. not `nil`)
// and that the error contains the specified substring.
//
//	actualObj, err := SomeFunction()
//	assert.ErrorContainsf(t, err,  expectedErrorSubString, "error message %s", "formatted")
func ErrorContainsf(t TestingT, theError error, contains string, msg string, args ...any) bool {
	return assert.ErrorContainsf(t, theError, contains, msg, args...)
}

// ErrorIs asserts that at least one of the errors in err's chain matches
// target. This is a wrapper for errors.Is.
func ErrorIs(t TestingT, err error, target error, msgAndArgs ...any) bool {
	return assert.ErrorIs(t, err, target, msgAndArgs...)
}

// ErrorIsf asserts that at least one of the errors in err's chain matches
// target. This is a wrapper for errors.Is.
func ErrorIsf(t TestingT, err error, target error, msg string, args ...any) bool {
	return assert.ErrorIsf(t, err, target, msg, args...)
}

// Errorf asserts that a function returned an error (i.e. not `nil`).
//
//	actualObj, err := SomeFunction()
//	assert.Errorf(t, err, "error message %s", "formatted")
func Errorf(t TestingT, err error, msg string, args ...any) bool {
	return assert.Errorf(t, err, msg, args...)
}

// Eventually asserts that given condition will be met in waitFor time,
// periodically checking target function each tick.
//
//	assert.Eventually(t, func() bool { return true; }, time.Second, 10*time.Millisecond)
func Eventually(t TestingT, condition func() bool, waitFor time.Duration, tick time.Duration, msgAndArgs ...any) bool {
	return assert.Eventually(t, condition, waitFor, tick, msgAndArgs...)
}

// EventuallyWithT asserts that given condition will be met in waitFor time,
// periodically checking target function each tick. In contrast to Eventually,
// it supplies a CollectT to the condition function, so that the condition
// function can use the CollectT to call other assertions. The condition is
// considered "met" if no errors are raised in a tick. The supplied CollectT
// collects all errors from one tick (if there are any). If the condition is
// not met before waitFor, the collected errors of the last tick are copied to
// t.
//
//	externalValue := false
//	go func() {
//		time.Sleep(8*time.Second)
//		externalValue = true
//	}()
//	assert.EventuallyWithT(t, func(c *assert.CollectT) {
//		// add assertions as needed; any assertion failure will fail the current tick
//		assert.True(c, externalValue, "expected 'externalValue' to be true")
//	}, 10*time.Second, 1*time.Second, "external state has not changed to 'true'; still false")
func EventuallyWithT(t TestingT, condition func(collect *CollectT), waitFor time.Duration, tick time.Duration, msgAndArgs ...any) bool {
	return assert.EventuallyWithT(t, condition, waitFor, tick, msgAndArgs...)
}

// EventuallyWithTf asserts that given condition will be met in waitFor time,
// periodically checking target function each tick. In contrast to Eventually,
// it supplies a CollectT to the condition function, so that the condition
// function can use the CollectT to call other assertions. The condition is
// considered "met" if no errors are raised in a tick. The supplied CollectT
// collects all errors from one tick (if there are any). If the condition is
// not met before waitFor, the collected errors of the last tick are copied to
// t.
//
//	externalValue := false
//	go func() {
//		time.Sleep(8*time.Second)
//		externalValue = true
//	}()
//	assert.EventuallyWithTf(t, func(c *assert.CollectT) {
//		// add assertions as needed; any assertion failure will fail the current tick
//		assert.True(c, externalValue, "expected 'externalValue' to be true")
//	}, 10*time.Second, 1*time.Second, "error message %s", "formatted")
func EventuallyWithTf(t TestingT, condition func(collect *CollectT), waitFor time.Duration, tick time.Duration, msg string, args ...any) bool {
	return assert.EventuallyWithTf(t, condition, waitFor, tick, msg, args...)
}

// Eventuallyf asserts that given condition will be met in waitFor time,
// periodically checking target function each tick.
//
//	assert.Eventuallyf(t, func() bool { return true; }, time.Second, 10*time.Millisecond, "error message %s", "formatted")
func Eventuallyf(t TestingT, condition func() bool, waitFor time.Duration, tick time.Duration, msg string, args ...any) bool {
	return assert.Eventuallyf(t, condition, waitFor, tick, msg, args...)
}

// Fail reports a failure through
func Fail(t TestingT, failureMessage string, msgAndArgs ...any) bool {
	return assert.Fail(t, failureMessage, msgAndArgs...)
}

// FailNow fails test
func FailNow(t TestingT, failureMessage string, msgAndArgs ...any) bool {
	return assert.FailNow(t, failureMessage, msgAndArgs...)
}

// FailNowf fails test
func FailNowf(t TestingT, failureMessage string, msg string, args ...any) bool {
	return assert.FailNowf(t, failureMessage, msg, args...)
}

// Failf reports a failure through
func Failf(t TestingT, failureMessage string, msg string, args ...any) bool {
	return assert.Failf(t, failureMessage, msg, args...)
}

// False asserts that the specified value is false.
//
//	assert.False(t, myBool)
func False(t TestingT, value bool, msgAndArgs ...any) bool {
	return assert.False(t, value, msgAndArgs...)
}

// Falsef asserts that the specified value is false.
//
//	assert.Falsef(t, myBool, "error message %s", "formatted")
func Falsef(t TestingT, value bool, msg string, args ...any) bool {
	return assert.Falsef(t, value, msg, args...)
}

// FileExists checks whether a file exists in the given path. It also fails if
// the path points to a directory or there is an error when trying to check the
// file.
func FileExists(t TestingT, path string, msgAndArgs ...any) bool {
	return assert.FileExists(t, path, msgAndArgs...)
}

// FileExistsf checks whether a file exists in the given path. It also fails if
// the path points to a directory or there is an error when trying to check the
// file.
func FileExistsf(t TestingT, path string, msg string, args ...any) bool {
	return assert.FileExistsf(t, path, msg, args...)
}

// HTTPBody is a helper that returns HTTP body of the response. It returns
// empty string if building a new request fails.
func HTTPBody(handler http.HandlerFunc, method, url string, values url.Values) string {
	return assert.HTTPBody(handler, method, url, values)
}

// HTTPBodyContains asserts that a specified handler returns a body that
// contains a string.
//
//	assert.HTTPBodyContains(t, myHandler, "GET", "www.google.com", nil, "I'm Feeling Lucky")
//
// Returns whether the assertion was successful (true) or not (false).
func HTTPBodyContains(t TestingT, handler http.HandlerFunc, method, url string, values url.Values, str string, msgAndArgs ...any) bool {
	return assert.HTTPBodyContains(t, handler, method, url, values, str, msgAndArgs...)
}

// HTTPBodyContainsf asserts that a specified handler returns a body that
// contains a string.
//
//	assert.HTTPBodyContainsf(t, myHandler, "GET", "www.google.com", nil, "I'm Feeling Lucky", "error message %s", "formatted")
//
// Returns whether the assertion was successful (true) or not (false).
func HTTPBodyContainsf(t TestingT, handler http.HandlerFunc, method string, url string, values url.Values, str string, msg string, args ...any) bool {
	return assert.HTTPBodyContainsf(t, handler, method, url, values, str, msg, args...)
}

// HTTPBodyNotContains asserts that a specified handler returns a body that
// does not contain a string.
//
//	assert.HTTPBodyNotContains(t, myHandler, "GET", "www.google.com", nil, "I'm Feeling Lucky")
//
// Returns whether the assertion was successful (true) or not (false).
func HTTPBodyNotContains(t TestingT, handler http.HandlerFunc, method, url string, values url.Values, str string, msgAndArgs ...any) bool {
	return assert.HTTPBodyNotContains(t, handler, method, url, values, str, msgAndArgs...)
}

// HTTPBodyNotContainsf asserts that a specified handler returns a body that
// does not contain a string.
//
//	assert.HTTPBodyNotContainsf(t, myHandler, "GET", "www.google.com", nil, "I'm Feeling Lucky", "error message %s", "formatted")
//
// Returns whether the assertion was successful (true) or not (false).
func HTTPBodyNotContainsf(t TestingT, handler http.HandlerFunc, method string, url string, values url.Values, str string, msg string, args ...any) bool {
	return assert.HTTPBodyNotContainsf(t, handler, method, url, values, str, msg, args...)
}

// HTTPError asserts that a specified handler returns an error status code.
//
//	assert.HTTPError(t, myHandler, "POST", "/a/b/c", url.Values{"a": []string{"b", "c"}}
//
// Returns whether the assertion was successful (true) or not (false).
func HTTPError(t TestingT, handler http.HandlerFunc, method, url string, values url.Values, msgAndArgs ...any) bool {
	return assert.HTTPError(t, handler, method, url, values, msgAndArgs...)
}

// HTTPErrorf asserts that a specified handler returns an error status code.
//
//	assert.HTTPErrorf(t, myHandler, "POST", "/a/b/c", url.Values{"a": []string{"b", "c"}}
//
// Returns whether the assertion was successful (true) or not (false).
func HTTPErrorf(t TestingT, handler http.HandlerFunc, method string, url string, values url.Values, msg string, args ...any) bool {
	return assert.HTTPErrorf(t, handler, method, url, values, msg, args...)
}

// HTTPRedirect asserts that a specified handler returns a redirect status
// code.
//
//	assert.HTTPRedirect(t, myHandler, "GET", "/a/b/c", url.Values{"a": []string{"b", "c"}}
//
// Returns whether the assertion was successful (true) or not (false).
func HTTPRedirect(t TestingT, handler http.HandlerFunc, method, url string, values url.Values, msgAndArgs ...any) bool {
	return assert.HTTPRedirect(t, handler, method, url, values, msgAndArgs...)
}

// HTTPRedirectf asserts that a specified handler returns a redirect status
// code.
//
//	assert.HTTPRedirectf(t, myHandler, "GET", "/a/b/c", url.Values{"a": []string{"b", "c"}}
//
// Returns whether the assertion was successful (true) or not (false).
func HTTPRedirectf(t TestingT, handler http.HandlerFunc, method string, url string, values url.Values, msg string, args ...any) bool {
	return assert.HTTPRedirectf(t, handler, method, url, values, msg, args...)
}

// HTTPStatusCode asserts that a specified handler returns a specified status
// code.
//
//	assert.HTTPStatusCode(t, myHandler, "GET", "/notImplemented", nil, 501)
//
// Returns whether the assertion was successful (true) or not (false).
func HTTPStatusCode(t TestingT, handler http.HandlerFunc, method, url string, values url.Values, expectedStatusCode int, msgAndArgs ...any) bool {
	return assert.HTTPStatusCode(t, handler, method, url, values, expectedStatusCode, msgAndArgs...)
}

// HTTPStatusCodef asserts that a specified handler returns a specified status
// code.
//
//	assert.HTTPStatusCodef(t, myHandler, "GET", "/notImplemented", nil, 501, "error message %s", "formatted")
//
// Returns whether the assertion was successful (true) or not (false).
func HTTPStatusCodef(t TestingT, handler http.HandlerFunc, method string, url string, values url.Values, expectedStatusCode int, msg string, args ...any) bool {
	return assert.HTTPStatusCodef(t, handler, method, url, values, expectedStatusCode, msg, args...)
}

// HTTPSuccess asserts that a specified handler returns a success status code.
//
//	assert.HTTPSuccess(t, myHandler, "POST", "http://www.google.com", nil)
//
// Returns whether the assertion was successful (true) or not (false).
func HTTPSuccess(t TestingT, handler http.HandlerFunc, method, url string, values url.Values, msgAndArgs ...any) bool {
	return assert.HTTPSuccess(t, handler, method, url, values, msgAndArgs...)
}

// HTTPSuccessf asserts that a specified handler returns a success status code.
//
//	assert.HTTPSuccessf(t, myHandler, "POST", "http://www.google.com", nil, "error message %s", "formatted")
//
// Returns whether the assertion was successful (true) or not (false).
func HTTPSuccessf(t TestingT, handler http.HandlerFunc, method string, url string, values url.Values, msg string, args ...any) bool {
	return assert.HTTPSuccessf(t, handler, method, url, values, msg, args...)
}

// Implements asserts that an object is implemented by the specified interface.
//
//	assert.Implements(t, (*MyInterface)(nil), new(MyObject))
func Implements(t TestingT, interfaceObject any, object any, msgAndArgs ...any) bool {
	return assert.Implements(t, interfaceObject, object, msgAndArgs...)
}

// Implementsf asserts that an object is implemented by the specified
// interface.
//
//	assert.Implementsf(t, (*MyInterface)(nil), new(MyObject), "error message %s", "formatted")
func Implementsf(t TestingT, interfaceObject any, object any, msg string, args ...any) bool {
	return assert.Implementsf(t, interfaceObject, object, msg, args...)
}

// IsDecreasing asserts that the collection is decreasing
//
//	assert.IsDecreasing(t, []int{2, 1, 0})
//	assert.IsDecreasing(t, []float{2, 1})
//	assert.IsDecreasing(t, []string{"b", "a"})
func IsDecreasing(t TestingT, object any, msgAndArgs ...any) bool {
	return assert.IsDecreasing(t, object, msgAndArgs...)
}

// IsDecreasingf asserts that the collection is decreasing
//
//	assert.IsDecreasingf(t, []int{2, 1, 0}, "error message %s", "formatted")
//	assert.IsDecreasingf(t, []float{2, 1}, "error message %s", "formatted")
//	assert.IsDecreasingf(t, []string{"b", "a"}, "error message %s", "formatted")
func IsDecreasingf(t TestingT, object any, msg string, args ...any) bool {
	return assert.IsDecreasingf(t, object, msg, args...)
}

// IsIncreasing asserts that the collection is increasing
//
//	assert.IsIncreasing(t, []int{1, 2, 3})
//	assert.IsIncreasing(t, []float{1, 2})
//	assert.IsIncreasing(t, []string{"a", "b"})
func IsIncreasing(t TestingT, object any, msgAndArgs ...any) bool {
	return assert.IsIncreasing(t, object, msgAndArgs...)
}

// IsIncreasingf asserts that the collection is increasing
//
//	assert.IsIncreasingf(t, []int{1, 2, 3}, "error message %s", "formatted")
//	assert.IsIncreasingf(t, []float{1, 2}, "error message %s", "formatted")
//	assert.IsIncreasingf(t, []string{"a", "b"}, "error message %s", "formatted")
func IsIncreasingf(t TestingT, object any, msg string, args ...any) bool {
	return assert.IsIncreasingf(t, object, msg, args...)
}

// IsNonDecreasing asserts that the collection is not decreasing
//
//	assert.IsNonDecreasing(t, []int{1, 1, 2})
//	assert.IsNonDecreasing(t, []float{1, 2})
//	assert.IsNonDecreasing(t, []string{"a", "b"})
func IsNonDecreasing(t TestingT, object any, msgAndArgs ...any) bool {
	return assert.IsNonDecreasing(t, object, msgAndArgs...)
}

// IsNonDecreasingf asserts that the collection is not decreasing
//
//	assert.IsNonDecreasingf(t, []int{1, 1, 2}, "error message %s", "formatted")
//	assert.IsNonDecreasingf(t, []float{1, 2}, "error message %s", "formatted")
//	assert.IsNonDecreasingf(t, []string{"a", "b"}, "error message %s", "formatted")
func IsNonDecreasingf(t TestingT, object any, msg string, args ...any) bool {
	return assert.IsNonDecreasingf(t, object, msg, args...)
}

// IsNonIncreasing asserts that the collection is not increasing
//
//	assert.IsNonIncreasing(t, []int{2, 1, 1})
//	assert.IsNonIncreasing(t, []float{2, 1})
//	assert.IsNonIncreasing(t, []string{"b", "a"})
func IsNonIncreasing(t TestingT, object any, msgAndArgs ...any) bool {
	return assert.IsNonIncreasing(t, object, msgAndArgs...)
}

// IsNonIncreasingf asserts that the collection is not increasing
//
//	assert.IsNonIncreasingf(t, []int{2, 1, 1}, "error message %s", "formatted")
//	assert.IsNonIncreasingf(t, []float{2, 1}, "error message %s", "formatted")
//	assert.IsNonIncreasingf(t, []string{"b", "a"}, "error message %s", "formatted")
func IsNonIncreasingf(t TestingT, object any, msg string, args ...any) bool {
	return assert.IsNonIncreasingf(t, object, msg, args...)
}

// JSONEq asserts that two JSON strings are equivalent.
//
//	assert.JSONEq(t, `{"hello": "world", "foo": "bar"}`, `{"foo": "bar", "hello": "world"}`)
func JSONEq(t TestingT, expected string, actual string, msgAndArgs ...any) bool {
	return assert.JSONEq(t, expected, actual, msgAndArgs...)
}

// JSONEqf asserts that two JSON strings are equivalent.
//
//	assert.JSONEqf(t, `{"hello": "world", "foo": "bar"}`, `{"foo": "bar", "hello": "world"}`, "error message %s", "formatted")
func JSONEqf(t TestingT, expected string, actual string, msg string, args ...any) bool {
	return assert.JSONEqf(t, expected, actual, msg, args...)
}

// Len asserts that the specified object has specific length. Len also fails if
// the object has a type that len() not accept.
//
//	assert.Len(t, mySlice, 3)
func Len(t TestingT, object any, length int, msgAndArgs ...any) bool {
	return assert.Len(t, object, length, msgAndArgs...)
}

// Lenf asserts that the specified object has specific length. Lenf also fails
// if the object has a type that len() not accept.
//
//	assert.Lenf(t, mySlice, 3, "error message %s", "formatted")
func Lenf(t TestingT, object any, length int, msg string, args ...any) bool {
	return assert.Lenf(t, object, length, msg, args...)
}

// Negative asserts that the specified element is negative
//
//	assert.Negative(t, -1)
//	assert.Negative(t, -1.23)
func Negative(t TestingT, e any, msgAndArgs ...any) bool {
	return assert.Negative(t, e, msgAndArgs...)
}

// Negativef asserts that the specified element is negative
//
//	assert.Negativef(t, -1, "error message %s", "formatted")
//	assert.Negativef(t, -1.23, "error message %s", "formatted")
func Negativef(t TestingT, e any, msg string, args ...any) bool {
	return assert.Negativef(t, e, msg, args...)
}

// Never asserts that the given condition doesn't satisfy in waitFor time,
// periodically checking the target function each tick.
//
//	assert.Never(t, func() bool { return false; }, time.Second, 10*time.Millisecond)
func Never(t TestingT, condition func() bool, waitFor time.Duration, tick time.Duration, msgAndArgs ...any) bool {
	return assert.Never(t, condition, waitFor, tick, msgAndArgs...)
}

// Neverf asserts that the given condition doesn't satisfy in waitFor time,
// periodically checking the target function each tick.
//
//	assert.Neverf(t, func() bool { return false; }, time.Second, 10*time.Millisecond, "error message %s", "formatted")
func Neverf(t TestingT, condition func() bool, waitFor time.Duration, tick time.Duration, msg string, args ...any) bool {
	return assert.Neverf(t, condition, waitFor, tick, msg, args...)
}

// Nil asserts that the specified object is nil.
//
//	assert.Nil(t, err)
func Nil(t TestingT, object any, msgAndArgs ...any) bool {
	return assert.Nil(t, object, msgAndArgs...)
}

// Nilf asserts that the specified object is nil.
//
//	assert.Nilf(t, err, "error message %s", "formatted")
func Nilf(t TestingT, object any, msg string, args ...any) bool {
	return assert.Nilf(t, object, msg, args...)
}

// NoDirExists checks whether a directory does not exist in the given path.
// It fails if the path points to an existing _directory_ only.
func NoDirExists(t TestingT, path string, msgAndArgs ...any) bool {
	return assert.NoDirExists(t, path, msgAndArgs...)
}

// NoDirExistsf checks whether a directory does not exist in the given path.
// It fails if the path points to an existing _directory_ only.
func NoDirExistsf(t TestingT, path string, msg string, args ...any) bool {
	return assert.NoDirExistsf(t, path, msg, args...)
}

// NoError asserts that a function returned no error (i.e. `nil`).
//
//	  actualObj, err := SomeFunction()
//	  if assert.NoError(t, err) {
//		   assert.Equal(t, expectedObj, actualObj)
//	  }
func NoError(t TestingT, err error, msgAndArgs ...any) bool {
	return assert.NoError(t, err, msgAndArgs...)
}

// NoErrorf asserts that a function returned no error (i.e. `nil`).
//
//	  actualObj, err := SomeFunction()
//	  if assert.NoErrorf(t, err, "error message %s", "formatted") {
//		   assert.Equal(t, expectedObj, actualObj)
//	  }
func NoErrorf(t TestingT, err error, msg string, args ...any) bool {
	return assert.NoErrorf(t, err, msg, args...)
}

// NoFileExists checks whether a file does not exist in a given path. It fails
// if the path points to an existing _file_ only.
func NoFileExists(t TestingT, path string, msgAndArgs ...any) bool {
	return assert.NoFileExists(t, path, msgAndArgs...)
}

// NoFileExistsf checks whether a file does not exist in a given path. It fails
// if the path points to an existing _file_ only.
func NoFileExistsf(t TestingT, path string, msg string, args ...any) bool {
	return assert.NoFileExistsf(t, path, msg, args...)
}

// NotEmpty asserts that the specified object is NOT Empty.
//
//	if assert.NotEmpty(t, obj) {
//	  assert.Equal(t, "two", obj[1])
//	}
func NotEmpty(t TestingT, object any, msgAndArgs ...any) bool {
	return assert.NotEmpty(t, object, msgAndArgs...)
}

// NotEmptyf asserts that the specified object is NOT Empty.
//
//	if assert.NotEmptyf(t, obj, "error message %s", "formatted") {
//	  assert.Equal(t, "two", obj[1])
//	}
func NotEmptyf(t TestingT, object any, msg string, args ...any) bool {
	return assert.NotEmptyf(t, object, msg, args...)
}

// NotErrorAs asserts that none of the errors in err's chain matches target,
// but if so, sets target to that error value.
func NotErrorAs(t TestingT, err error, target any, msgAndArgs ...any) bool {
	return assert.NotErrorAs(t, err, target, msgAndArgs...)
}

// NotErrorAsf asserts that none of the errors in err's chain matches target,
// but if so, sets target to that error value.
func NotErrorAsf(t TestingT, err error, target any, msg string, args ...any) bool {
	return assert.NotErrorAsf(t, err, target, msg, args...)
}

// NotErrorIs asserts that none of the errors in err's chain matches target.
// This is a wrapper for errors.Is.
func NotErrorIs(t TestingT, err error, target error, msgAndArgs ...any) bool {
	return assert.NotErrorIs(t, err, target, msgAndArgs...)
}

// NotErrorIsf asserts that none of the errors in err's chain matches target.
// This is a wrapper for errors.Is.
func NotErrorIsf(t TestingT, err error, target error, msg string, args ...any) bool {
	return assert.NotErrorIsf(t, err, target, msg, args...)
}

// NotNil asserts that the specified object is not nil.
//
//	assert.NotNil(t, err)
func NotNil(t TestingT, object any, msgAndArgs ...any) bool {
	return assert.NotNil(t, object, msgAndArgs...)
}

// NotNilf asserts that the specified object is not nil.
//
//	assert.NotNilf(t, err, "error message %s", "formatted")
func NotNilf(t TestingT, object any, msg string, args ...any) bool {
	return assert.NotNilf(t, object, msg, args...)
}

// NotPanics asserts that the code inside the specified PanicTestFunc does NOT
// panic.
//
//	assert.NotPanics(t, func(){ RemainCalm() })
func NotPanics(t TestingT, f PanicTestFunc, msgAndArgs ...any) bool {
	return assert.NotPanics(t, f, msgAndArgs...)
}

// NotPanicsf asserts that the code inside the specified PanicTestFunc does NOT
// panic.
//
//	assert.NotPanicsf(t, func(){ RemainCalm() }, "error message %s", "formatted")
func NotPanicsf(t TestingT, f PanicTestFunc, msg string, args ...any) bool {
	return assert.NotPanicsf(t, f, msg, args...)
}

// NotZero asserts that i is not the zero value for its type.
func NotZero(t TestingT, i any, msgAndArgs ...any) bool {
	return assert.NotZero(t, i, msgAndArgs...)
}

// NotZerof asserts that i is not the zero value for its type.
func NotZerof(t TestingT, i any, msg string, args ...any) bool {
	return assert.NotZerof(t, i, msg, args...)
}

// ObjectsAreEqual determines if two objects are considered equal.
//
// This function does no assertion of any kind.
func ObjectsAreEqual(expected any, actual any) bool {
	return assert.ObjectsAreEqual(expected, actual)
}

// ObjectsAreEqualValues gets whether two objects are equal, or if their values
// are equal.
func ObjectsAreEqualValues(expected any, actual any) bool {
	return assert.ObjectsAreEqualValues(expected, actual)
}

// ObjectsExportedFieldsAreEqual determines if the exported (public) fields of
// two objects are considered equal. This comparison of only exported fields is
// applied recursively to nested data structures.
//
// This function does no assertion of any kind.
//
// Deprecated: Use EqualExportedValues instead.
func ObjectsExportedFieldsAreEqual(expected any, actual any) bool {
	return assert.ObjectsExportedFieldsAreEqual(expected, actual)
}

// Panics asserts that the code inside the specified PanicTestFunc panics.
//
//	assert.Panics(t, func(){ GoCrazy() })
func Panics(t TestingT, f PanicTestFunc, msgAndArgs ...any) bool {
	return assert.Panics(t, f, msgAndArgs...)
}

// PanicsWithError asserts that the code inside the specified PanicTestFunc
// panics, and that the recovered panic value is an error that satisfies the
// EqualError comparison.
//
//	assert.PanicsWithError(t, "crazy error", func(){ GoCrazy() })
func PanicsWithError(t TestingT, errString string, f PanicTestFunc, msgAndArgs ...any) bool {
	return assert.PanicsWithError(t, errString, f, msgAndArgs...)
}

// PanicsWithErrorf asserts that the code inside the specified PanicTestFunc
// panics, and that the recovered panic value is an error that satisfies the
// EqualError comparison.
//
//	assert.PanicsWithErrorf(t, "crazy error", func(){ GoCrazy() }, "error message %s", "formatted")
func PanicsWithErrorf(t TestingT, errString string, f PanicTestFunc, msg string, args ...any) bool {
	return assert.PanicsWithErrorf(t, errString, f, msg, args...)
}

// PanicsWithValue asserts that the code inside the specified PanicTestFunc
// panics, and that the recovered panic value equals the expected panic value.
//
//	assert.PanicsWithValue(t, "crazy error", func(){ GoCrazy() })
func PanicsWithValue(t TestingT, expected any, f PanicTestFunc, msgAndArgs ...any) bool {
	return assert.PanicsWithValue(t, expected, f, msgAndArgs...)
}

// PanicsWithValuef asserts that the code inside the specified PanicTestFunc
// panics, and that the recovered panic value equals the expected panic value.
//
//	assert.PanicsWithValuef(t, "crazy error", func(){ GoCrazy() }, "error message %s", "formatted")
func PanicsWithValuef(t TestingT, expected any, f PanicTestFunc, msg string, args ...any) bool {
	return assert.PanicsWithValuef(t, expected, f, msg, args...)
}

// Panicsf asserts that the code inside the specified PanicTestFunc panics.
//
//	assert.Panicsf(t, func(){ GoCrazy() }, "error message %s", "formatted")
func Panicsf(t TestingT, f PanicTestFunc, msg string, args ...any) bool {
	return assert.Panicsf(t, f, msg, args...)
}

// Positive asserts that the specified element is positive
//
//	assert.Positive(t, 1)
//	assert.Positive(t, 1.23)
func Positive(t TestingT, e any, msgAndArgs ...any) bool {
	return assert.Positive(t, e, msgAndArgs...)
}

// Positivef asserts that the specified element is positive
//
//	assert.Positivef(t, 1, "error message %s", "formatted")
//	assert.Positivef(t, 1.23, "error message %s", "formatted")
func Positivef(t TestingT, e any, msg string, args ...any) bool {
	return assert.Positivef(t, e, msg, args...)
}

// True asserts that the specified value is true.
//
//	assert.True(t, myBool)
func True(t TestingT, value bool, msgAndArgs ...any) bool {
	return assert.True(t, value, msgAndArgs...)
}

// Truef asserts that the specified value is true.
//
//	assert.Truef(t, myBool, "error message %s", "formatted")
func Truef(t TestingT, value bool, msg string, args ...any) bool {
	return assert.Truef(t, value, msg, args...)
}

// WithinDuration asserts that the two times are within duration delta of each
// other.
//
//	assert.WithinDuration(t, time.Now(), time.Now(), 10*time.Second)
func WithinDuration(t TestingT, expected time.Time, actual time.Time, delta time.Duration, msgAndArgs ...any) bool {
	return assert.WithinDuration(t, expected, actual, delta, msgAndArgs...)
}

// WithinDurationf asserts that the two times are within duration delta of each
// other.
//
//	assert.WithinDurationf(t, time.Now(), time.Now(), 10*time.Second, "error message %s", "formatted")
func WithinDurationf(t TestingT, expected time.Time, actual time.Time, delta time.Duration, msg string, args ...any) bool {
	return assert.WithinDurationf(t, expected, actual, delta, msg, args...)
}

// WithinRange asserts that a time is within a time range (inclusive).
//
//	assert.WithinRange(t, time.Now(), time.Now().Add(-time.Second), time.Now().Add(time.Second))
func WithinRange(t TestingT, actual time.Time, start time.Time, end time.Time, msgAndArgs ...any) bool {
	return assert.WithinRange(t, actual, start, end, msgAndArgs...)
}

// WithinRangef asserts that a time is within a time range (inclusive).
//
//	assert.WithinRangef(t, time.Now(), time.Now().Add(-time.Second), time.Now().Add(time.Second), "error message %s", "formatted")
func WithinRangef(t TestingT, actual time.Time, start time.Time, end time.Time, msg string, args ...any) bool {
	return assert.WithinRangef(t, actual, start, end, msg, args...)
}

// YAMLEq asserts that two YAML strings are equivalent.
func YAMLEq(t TestingT, expected string, actual string, msgAndArgs ...any) bool {
	return assert.YAMLEq(t, expected, actual, msgAndArgs...)
}

// YAMLEqf asserts that two YAML strings are equivalent.
func YAMLEqf(t TestingT, expected string, actual string, msg string, args ...any) bool {
	return assert.YAMLEqf(t, expected, actual, msg, args...)
}

// Zero asserts that i is the zero value for its type.
func Zero(t TestingT, i any, msgAndArgs ...any) bool {
	return assert.Zero(t, i, msgAndArgs...)
}

// Zerof asserts that i is the zero value for its type.
func Zerof(t TestingT, i any, msg string, args ...any) bool {
	return assert.Zerof(t, i, msg, args...)
}
