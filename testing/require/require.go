// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package require

import (
	"iter"
	"net/http"
	"net/url"
	"slices"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestingT = require.TestingT
type ErrorAssertionFunc = require.ErrorAssertionFunc
type ValueAssertionFunc = require.ValueAssertionFunc
type ComparisonAssertionFunc = require.ComparisonAssertionFunc
type BoolAssertionFunc = require.BoolAssertionFunc

// Generic wrapper functions for functions taking two interface{} arguments:

// Contains asserts that the specified string, list(array, slice...) or iterator
// contains the specified substring or element.
//
//	require.Contains(t, "Hello World", "World")
//	require.Contains(t, ["Hello", "World"], "World")
//	require.Contains(t, maps.Keys({"Hello": "World"}), "Hello")
func Contains[T2 comparable, T1 ~string | ~[]T2 | iter.Seq[T2]](t TestingT, s T1, contains T2, msgAndArgs ...any) {
	if seq, ok := any(s).(iter.Seq[T2]); ok {
		require.Contains(t, slices.Collect(seq), contains, msgAndArgs...)
		return
	}
	require.Contains(t, s, contains, msgAndArgs...)
}

// Containsf asserts that the specified string, list(array, slice...) or iterator
// contains the specified substring or element.
//
//	require.Containsf(t, "Hello World", "World", "error message %s", "formatted")
//	require.Containsf(t, ["Hello", "World"], "World", "error message %s", "formatted")
//	require.Containsf(t, maps.Keys({"Hello": "World"}), "Hello", "error message %s", "formatted")
func Containsf[T2 comparable, T1 ~string | ~[]T2 | iter.Seq[T2]](t TestingT, s T1, contains T2, msg string, args ...any) {
	if seq, ok := any(s).(iter.Seq[T2]); ok {
		require.Containsf(t, slices.Collect(seq), contains, msg, args...)
		return
	}
	require.Containsf(t, s, contains, msg, args...)
}

// ElementsMatch asserts that the specified listA(array, slice...) is equal
// to specified listB(array, slice...) ignoring the order of the elements.
// If there are duplicate elements, the number of appearances of each of them
// in both lists should match.
//
// require.ElementsMatch(t, [1, 3, 2, 3], [1, 3, 3, 2])
func ElementsMatch[T1, T2 any](t TestingT, listA T1, listB T2, msgAndArgs ...any) {
	require.ElementsMatch(t, listA, listB, msgAndArgs...)
}

// ElementsMatchf asserts that the specified listA(array, slice...) is equal
// to specified listB(array, slice...) ignoring the order of the elements.
// If there are duplicate elements, the number of appearances of each of them
// in both lists should match.
//
// require.ElementsMatchf(t, [1, 3, 2, 3], [1, 3, 3, 2], "error message %s",
// "formatted")
func ElementsMatchf[T1, T2 any](t TestingT, listA T1, listB T2, msg string, args ...any) {
	require.ElementsMatchf(t, listA, listB, msg, args...)
}

// Equal asserts that two objects are equal.
//
//	require.Equal(t, 123, 123)
//
// Pointer variable equality is determined based on the equality of the
// referenced values (as opposed to the memory addresses). Function equality
// cannot be determined and will always fail.
func Equal[T any](t TestingT, expected T, actual T, msgAndArgs ...any) {
	require.Equal(t, expected, actual, msgAndArgs...)
}

// Equalf asserts that two objects are equal.
//
//	require.Equalf(t, 123, 123, "error message %s", "formatted")
//
// Pointer variable equality is determined based on the equality of the
// referenced values (as opposed to the memory addresses). Function equality
// cannot be determined and will always fail.
func Equalf[T any](t TestingT, expected T, actual T, msg string, args ...any) {
	require.Equalf(t, expected, actual, msg, args...)
}

// EqualExportedValues asserts that the types of two objects are equal and
// their public fields are also equal. This is useful for comparing structs
// that have private fields that could potentially differ.
//
//	 type S struct {
//		Exported     	int
//		notExported   	int
//	 }
//	 require.EqualExportedValues(t, S{1, 2}, S{1, 3}) => true
//	 require.EqualExportedValues(t, S{1, 2}, S{2, 3}) => false
func EqualExportedValues[T any](t TestingT, expected T, actual T, msgAndArgs ...any) {
	require.EqualExportedValues(t, expected, actual, msgAndArgs...)
}

// EqualExportedValuesf asserts that the types of two objects are equal and
// their public fields are also equal. This is useful for comparing structs
// that have private fields that could potentially differ.
//
//	 type S struct {
//		Exported     	int
//		notExported   	int
//	 }
//	 require.EqualExportedValuesf(t, S{1, 2}, S{1, 3}, "error message %s", "formatted") => true
//	 require.EqualExportedValuesf(t, S{1, 2}, S{2, 3}, "error message %s", "formatted") => false
func EqualExportedValuesf[T any](t TestingT, expected T, actual T, msg string, args ...any) {
	require.EqualExportedValuesf(t, expected, actual, msg, args...)
}

// EqualValues asserts that two objects are equal or convertible to the larger
// type and equal.
//
//	require.EqualValues(t, uint32(123), int32(123))
func EqualValues[T any](t TestingT, expected T, actual T, msgAndArgs ...any) {
	require.EqualValues(t, expected, actual, msgAndArgs...)
}

// EqualValuesf asserts that two objects are equal or convertible to the larger
// type and equal.
//
//	require.EqualValuesf(t, uint32(123), int32(123), "error message %s", "formatted")
func EqualValuesf[T any](t TestingT, expected T, actual T, msg string, args ...any) {
	require.EqualValuesf(t, expected, actual, msg, args...)
}

// Exactly asserts that two objects are equal in value and type.
//
//	require.Exactly(t, int32(123), int64(123))
func Exactly[T any](t TestingT, expected T, actual T, msgAndArgs ...any) {
	require.Exactly(t, expected, actual, msgAndArgs...)
}

// Exactlyf asserts that two objects are equal in value and type.
//
//	require.Exactlyf(t, int32(123), int64(123), "error message %s", "formatted")
func Exactlyf[T any](t TestingT, expected T, actual T, msg string, args ...any) {
	require.Exactlyf(t, expected, actual, msg, args...)
}

// Greater asserts that the first element is greater than the second
//
//	require.Greater(t, 2, 1)
//	require.Greater(t, float64(2), float64(1))
//	require.Greater(t, "b", "a")
func Greater[T any](t TestingT, e1 T, e2 T, msgAndArgs ...any) {
	require.Greater(t, e1, e2, msgAndArgs...)
}

// Greaterf asserts that the first element is greater than the second
//
//	require.Greaterf(t, 2, 1, "error message %s", "formatted")
//	require.Greaterf(t, float64(2), float64(1), "error message %s", "formatted")
//	require.Greaterf(t, "b", "a", "error message %s", "formatted")
func Greaterf[T any](t TestingT, e1 T, e2 T, msg string, args ...any) {
	require.Greaterf(t, e1, e2, msg, args...)
}

// GreaterOrEqual asserts that the first element is greater than or equal to
// the second
//
//	require.GreaterOrEqual(t, 2, 1)
//	require.GreaterOrEqual(t, 2, 2)
//	require.GreaterOrEqual(t, "b", "a")
//	require.GreaterOrEqual(t, "b", "b")
func GreaterOrEqual[T any](t TestingT, e1 T, e2 T, msgAndArgs ...any) {
	require.GreaterOrEqual(t, e1, e2, msgAndArgs...)
}

// GreaterOrEqualf asserts that the first element is greater than or equal to
// the second
//
//	require.GreaterOrEqualf(t, 2, 1, "error message %s", "formatted")
//	require.GreaterOrEqualf(t, 2, 2, "error message %s", "formatted")
//	require.GreaterOrEqualf(t, "b", "a", "error message %s", "formatted")
//	require.GreaterOrEqualf(t, "b", "b", "error message %s", "formatted")
func GreaterOrEqualf[T any](t TestingT, e1 T, e2 T, msg string, args ...any) {
	require.GreaterOrEqualf(t, e1, e2, msg, args...)
}

// IsNotType asserts that the specified objects are not of the same type.
//
//	require.IsNotType(t, &NotMyStruct{}, &MyStruct{})
func IsNotType[T1, T2 any](t TestingT, theType T1, object T2, msgAndArgs ...any) {
	require.IsNotType(t, theType, object, msgAndArgs...)
}

// IsNotTypef asserts that the specified objects are not of the same type.
//
//	require.IsNotTypef(t, &NotMyStruct{}, &MyStruct{}, "error message %s", "formatted")
func IsNotTypef[T1, T2 any](t TestingT, theType T1, object T2, msg string, args ...any) {
	require.IsNotTypef(t, theType, object, msg, args...)
}

// IsType asserts that the specified objects are of the same type.
//
//	require.IsType(t, &MyStruct{}, &MyStruct{})
func IsType[T1, T2 any](t TestingT, expectedType T1, object T2, msgAndArgs ...any) {
	require.IsType(t, expectedType, object, msgAndArgs...)
}

// IsTypef asserts that the specified objects are of the same type.
//
//	require.IsTypef(t, &MyStruct{}, &MyStruct{}, "error message %s", "formatted")
func IsTypef[T1, T2 any](t TestingT, expectedType T1, object T2, msg string, args ...any) {
	require.IsTypef(t, expectedType, object, msg, args...)
}

// Less asserts that the first element is less than the second
//
//	require.Less(t, 1, 2)
//	require.Less(t, float64(1), float64(2))
//	require.Less(t, "a", "b")
func Less[T any](t TestingT, e1 T, e2 T, msgAndArgs ...any) {
	require.Less(t, e1, e2, msgAndArgs...)
}

// Lessf asserts that the first element is less than the second
//
//	require.Lessf(t, 1, 2, "error message %s", "formatted")
//	require.Lessf(t, float64(1), float64(2), "error message %s", "formatted")
//	require.Lessf(t, "a", "b", "error message %s", "formatted")
func Lessf[T any](t TestingT, e1 T, e2 T, msg string, args ...any) {
	require.Lessf(t, e1, e2, msg, args...)
}

// LessOrEqual asserts that the first element is less than or equal to the
// second
//
//	require.LessOrEqual(t, 1, 2)
//	require.LessOrEqual(t, 2, 2)
//	require.LessOrEqual(t, "a", "b")
//	require.LessOrEqual(t, "b", "b")
func LessOrEqual[T any](t TestingT, e1 T, e2 T, msgAndArgs ...any) {
	require.LessOrEqual(t, e1, e2, msgAndArgs...)
}

// LessOrEqualf asserts that the first element is less than or equal to the
// second
//
//	require.LessOrEqualf(t, 1, 2, "error message %s", "formatted")
//	require.LessOrEqualf(t, 2, 2, "error message %s", "formatted")
//	require.LessOrEqualf(t, "a", "b", "error message %s", "formatted")
//	require.LessOrEqualf(t, "b", "b", "error message %s", "formatted")
func LessOrEqualf[T any](t TestingT, e1 T, e2 T, msg string, args ...any) {
	require.LessOrEqualf(t, e1, e2, msg, args...)
}

// NotContains asserts that the specified string, list(array, slice...) or iterator
// does NOT contain the specified substring or element.
//
//	require.NotContains(t, "Hello World", "Earth")
//	require.NotContains(t, ["Hello", "World"], "Earth")
//	require.NotContains(t, maps.Keys({"Hello": "World"}), "Earth")
func NotContains[T2 comparable, T1 ~string | ~[]T2 | iter.Seq[T2]](t TestingT, s T1, contains T2, msgAndArgs ...any) {
	if seq, ok := any(s).(iter.Seq[T2]); ok {
		require.NotContains(t, slices.Collect(seq), contains, msgAndArgs...)
		return
	}
	require.NotContains(t, s, contains, msgAndArgs...)
}

// NotContainsf asserts that the specified string, list(array, slice...) or iterator
// does NOT contain the specified substring or element.
//
//	require.NotContainsf(t, "Hello World", "Earth", "error message %s", "formatted")
//	require.NotContainsf(t, ["Hello", "World"], "Earth", "error message %s", "formatted")
//	require.NotContainsf(t, maps.Keys({"Hello": "World"}), "Earth", "error message %s", "formatted")
func NotContainsf[T2 comparable, T1 ~string | ~[]T2 | iter.Seq[T2]](t TestingT, s T1, contains T2, msg string, args ...any) {
	if seq, ok := any(s).(iter.Seq[T2]); ok {
		require.NotContainsf(t, slices.Collect(seq), contains, msg, args...)
		return
	}
	require.NotContainsf(t, s, contains, msg, args...)
}

// NotElementsMatch asserts that the specified listA(array, slice...) is
// NOT equal to specified listB(array, slice...) ignoring the order of the
// elements. If there are duplicate elements, the number of appearances of each
// of them in both lists should not match. This is an inverse of ElementsMatch.
//
// require.NotElementsMatch(t, [1, 1, 2, 3], [1, 1, 2, 3]) -> false
//
// require.NotElementsMatch(t, [1, 1, 2, 3], [1, 2, 3]) -> true
//
// require.NotElementsMatch(t, [1, 2, 3], [1, 2, 4]) -> true
func NotElementsMatch[T1, T2 any](t TestingT, listA T1, listB T2, msgAndArgs ...any) {
	require.NotElementsMatch(t, listA, listB, msgAndArgs...)
}

// NotElementsMatchf asserts that the specified listA(array, slice...) is
// NOT equal to specified listB(array, slice...) ignoring the order of the
// elements. If there are duplicate elements, the number of appearances of each
// of them in both lists should not match. This is an inverse of ElementsMatch.
//
// require.NotElementsMatchf(t, [1, 1, 2, 3], [1, 1, 2, 3], "error message %s",
// "formatted") -> false
//
// require.NotElementsMatchf(t, [1, 1, 2, 3], [1, 2, 3], "error message %s",
// "formatted") -> true
//
// require.NotElementsMatchf(t, [1, 2, 3], [1, 2, 4], "error message %s",
// "formatted") -> true
func NotElementsMatchf[T1, T2 any](t TestingT, listA T1, listB T2, msg string, args ...any) {
	require.NotElementsMatchf(t, listA, listB, msg, args...)
}

// NotEqual asserts that the specified values are NOT equal.
//
//	require.NotEqual(t, obj1, obj2)
//
// Pointer variable equality is determined based on the equality of the
// referenced values (as opposed to the memory addresses).
func NotEqual[T any](t TestingT, expected T, actual T, msgAndArgs ...any) {
	require.NotEqual(t, expected, actual, msgAndArgs...)
}

// NotEqualValues asserts that two objects are not equal even when converted to
// the same type
//
//	require.NotEqualValues(t, obj1, obj2)
func NotEqualValues[T any](t TestingT, expected T, actual T, msgAndArgs ...any) {
	require.NotEqualValues(t, expected, actual, msgAndArgs...)
}

// NotEqualValuesf asserts that two objects are not equal even when converted
// to the same type
//
//	require.NotEqualValuesf(t, obj1, obj2, "error message %s", "formatted")
func NotEqualValuesf[T any](t TestingT, expected T, actual T, msg string, args ...any) {
	require.NotEqualValuesf(t, expected, actual, msg, args...)
}

// NotEqualf asserts that the specified values are NOT equal.
//
//	require.NotEqualf(t, obj1, obj2, "error message %s", "formatted")
//
// Pointer variable equality is determined based on the equality of the
// referenced values (as opposed to the memory addresses).
func NotEqualf[T any](t TestingT, expected T, actual T, msg string, args ...any) {
	require.NotEqualf(t, expected, actual, msg, args...)
}

// NotImplements asserts that an object does not implement the specified
// interface.
//
//	require.NotImplements(t, (*MyInterface)(nil), new(MyObject))
func NotImplements[T1, T2 any](t TestingT, interfaceObject T1, object T2, msgAndArgs ...any) {
	require.NotImplements(t, interfaceObject, object, msgAndArgs...)
}

// NotImplementsf asserts that an object does not implement the specified
// interface.
//
//	require.NotImplementsf(t, (*MyInterface)(nil), new(MyObject), "error message %s", "formatted")
func NotImplementsf[T1, T2 any](t TestingT, interfaceObject T1, object T2, msg string, args ...any) {
	require.NotImplementsf(t, interfaceObject, object, msg, args...)
}

// NotRegexp asserts that a specified regexp does not match a string.
//
//	require.NotRegexp(t, regexp.MustCompile("starts"), "it's starting")
//	require.NotRegexp(t, "^start", "it's not starting")
func NotRegexp[T1, T2 any](t TestingT, rx T1, str T2, msgAndArgs ...any) {
	require.NotRegexp(t, rx, str, msgAndArgs...)
}

// NotRegexpf asserts that a specified regexp does not match a string.
//
//	require.NotRegexpf(t, regexp.MustCompile("starts"), "it's starting", "error message %s", "formatted")
//	require.NotRegexpf(t, "^start", "it's not starting", "error message %s", "formatted")
func NotRegexpf[T1, T2 any](t TestingT, rx T1, str T2, msg string, args ...any) {
	require.NotRegexpf(t, rx, str, msg, args...)
}

// NotSame asserts that two pointers do not reference the same object.
//
//	require.NotSame(t, ptr1, ptr2)
//
// Both arguments must be pointer variables. Pointer variable sameness is
// determined based on the equality of both type and value.
func NotSame[T any](t TestingT, expected T, actual T, msgAndArgs ...any) {
	require.NotSame(t, expected, actual, msgAndArgs...)
}

// NotSamef asserts that two pointers do not reference the same object.
//
//	require.NotSamef(t, ptr1, ptr2, "error message %s", "formatted")
//
// Both arguments must be pointer variables. Pointer variable sameness is
// determined based on the equality of both type and value.
func NotSamef[T any](t TestingT, expected T, actual T, msg string, args ...any) {
	require.NotSamef(t, expected, actual, msg, args...)
}

// NotSubset asserts that the list (array, slice, or map) does NOT contain
// all elements given in the subset (array, slice, or map). Map elements are
// key-value pairs unless compared with an array or slice where only the map
// key is evaluated.
//
//	require.NotSubset(t, [1, 3, 4], [1, 2])
//	require.NotSubset(t, {"x": 1, "y": 2}, {"z": 3})
//	require.NotSubset(t, [1, 3, 4], {1: "one", 2: "two"})
//	require.NotSubset(t, {"x": 1, "y": 2}, ["z"])
func NotSubset[T1, T2 any](t TestingT, list T1, subset T2, msgAndArgs ...any) {
	require.NotSubset(t, list, subset, msgAndArgs...)
}

// NotSubsetf asserts that the list (array, slice, or map) does NOT contain
// all elements given in the subset (array, slice, or map). Map elements are
// key-value pairs unless compared with an array or slice where only the map
// key is evaluated.
//
//	require.NotSubsetf(t, [1, 3, 4], [1, 2], "error message %s", "formatted")
//	require.NotSubsetf(t, {"x": 1, "y": 2}, {"z": 3}, "error message %s", "formatted")
//	require.NotSubsetf(t, [1, 3, 4], {1: "one", 2: "two"}, "error message %s", "formatted")
//	require.NotSubsetf(t, {"x": 1, "y": 2}, ["z"], "error message %s", "formatted")
func NotSubsetf[T1, T2 any](t TestingT, list T1, subset T2, msg string, args ...any) {
	require.NotSubsetf(t, list, subset, msg, args...)
}

// Regexp asserts that a specified regexp matches a string.
//
//	require.Regexp(t, regexp.MustCompile("start"), "it's starting")
//	require.Regexp(t, "start...$", "it's not starting")
func Regexp[T1, T2 any](t TestingT, rx T1, str T2, msgAndArgs ...any) {
	require.Regexp(t, rx, str, msgAndArgs...)
}

// Regexpf asserts that a specified regexp matches a string.
//
//	require.Regexpf(t, regexp.MustCompile("start"), "it's starting", "error message %s", "formatted")
//	require.Regexpf(t, "start...$", "it's not starting", "error message %s", "formatted")
func Regexpf[T1, T2 any](t TestingT, rx T1, str T2, msg string, args ...any) {
	require.Regexpf(t, rx, str, msg, args...)
}

// Same asserts that two pointers reference the same object.
//
//	require.Same(t, ptr1, ptr2)
//
// Both arguments must be pointer variables. Pointer variable sameness is
// determined based on the equality of both type and value.
func Same[T any](t TestingT, expected T, actual T, msgAndArgs ...any) {
	require.Same(t, expected, actual, msgAndArgs...)
}

// Samef asserts that two pointers reference the same object.
//
//	require.Samef(t, ptr1, ptr2, "error message %s", "formatted")
//
// Both arguments must be pointer variables. Pointer variable sameness is
// determined based on the equality of both type and value.
func Samef[T any](t TestingT, expected T, actual T, msg string, args ...any) {
	require.Samef(t, expected, actual, msg, args...)
}

// Subset asserts that the list (array, slice, or map) contains all elements
// given in the subset (array, slice, or map). Map elements are key-value pairs
// unless compared with an array or slice where only the map key is evaluated.
//
//	require.Subset(t, [1, 2, 3], [1, 2])
//	require.Subset(t, {"x": 1, "y": 2}, {"x": 1})
//	require.Subset(t, [1, 2, 3], {1: "one", 2: "two"})
//	require.Subset(t, {"x": 1, "y": 2}, ["x"])
func Subset[T1, T2 any](t TestingT, list T1, subset T2, msgAndArgs ...any) {
	require.Subset(t, list, subset, msgAndArgs...)
}

// Subsetf asserts that the list (array, slice, or map) contains all elements
// given in the subset (array, slice, or map). Map elements are key-value pairs
// unless compared with an array or slice where only the map key is evaluated.
//
//	require.Subsetf(t, [1, 2, 3], [1, 2], "error message %s", "formatted")
//	require.Subsetf(t, {"x": 1, "y": 2}, {"x": 1}, "error message %s", "formatted")
//	require.Subsetf(t, [1, 2, 3], {1: "one", 2: "two"}, "error message %s", "formatted")
//	require.Subsetf(t, {"x": 1, "y": 2}, ["x"], "error message %s", "formatted")
func Subsetf[T1, T2 any](t TestingT, list T1, subset T2, msg string, args ...any) {
	require.Subsetf(t, list, subset, msg, args...)
}

// InDelta asserts that the two numerals are within delta of each other.
//
//	require.InDelta(t, math.Pi, 22/7.0, 0.01)
func InDelta[T any](t TestingT, expected T, actual T, delta float64, msgAndArgs ...any) {
	require.InDelta(t, expected, actual, delta, msgAndArgs...)
}

// InDeltaSlice is the same as InDelta, except it compares two slices.
func InDeltaSlice[T any](t TestingT, expected T, actual T, delta float64, msgAndArgs ...any) {
	require.InDeltaSlice(t, expected, actual, delta, msgAndArgs...)
}

// InDeltaSlicef is the same as InDelta, except it compares two slices.
func InDeltaSlicef[T any](t TestingT, expected T, actual T, delta float64, msg string, args ...any) {
	require.InDeltaSlicef(t, expected, actual, delta, msg, args...)
}

// InDeltaf asserts that the two numerals are within delta of each other.
//
//	require.InDeltaf(t, math.Pi, 22/7.0, 0.01, "error message %s", "formatted")
func InDeltaf[T any](t TestingT, expected T, actual T, delta float64, msg string, args ...any) {
	require.InDeltaf(t, expected, actual, delta, msg, args...)
}

// InDeltaMapValues is the same as InDelta, but it compares all values between
// two maps. Both maps must have exactly the same keys.
func InDeltaMapValues[T any](t TestingT, expected T, actual T, delta float64, msgAndArgs ...any) {
	require.InDeltaMapValues(t, expected, actual, delta, msgAndArgs...)
}

// InDeltaMapValuesf is the same as InDelta, but it compares all values between
// two maps. Both maps must have exactly the same keys.
func InDeltaMapValuesf[T any](t TestingT, expected T, actual T, delta float64, msg string, args ...any) {
	require.InDeltaMapValuesf(t, expected, actual, delta, msg, args...)
}

// InEpsilon asserts that expected and actual have a relative error less than
// epsilon
func InEpsilon[T any](t TestingT, expected T, actual T, epsilon float64, msgAndArgs ...any) {
	require.InEpsilon(t, expected, actual, epsilon, msgAndArgs...)
}

// InEpsilonSlice is the same as InEpsilon, except it compares each value from
// two slices.
func InEpsilonSlice[T any](t TestingT, expected T, actual T, epsilon float64, msgAndArgs ...any) {
	require.InEpsilonSlice(t, expected, actual, epsilon, msgAndArgs...)
}

// InEpsilonSlicef is the same as InEpsilon, except it compares each value from
// two slices.
func InEpsilonSlicef[T any](t TestingT, expected T, actual T, epsilon float64, msg string, args ...any) {
	require.InEpsilonSlicef(t, expected, actual, epsilon, msg, args...)
}

// InEpsilonf asserts that expected and actual have a relative error less than
// epsilon
func InEpsilonf[T any](t TestingT, expected T, actual T, epsilon float64, msg string, args ...any) {
	require.InEpsilonf(t, expected, actual, epsilon, msg, args...)
}

// Pass-through of all other functions:

// Condition uses a Comparison to assert a complex condition.
func Condition(t TestingT, comp assert.Comparison, msgAndArgs ...any) {
	require.Condition(t, comp, msgAndArgs...)
}

// Conditionf uses a Comparison to assert a complex condition.
func Conditionf(t TestingT, comp assert.Comparison, msg string, args ...any) {
	require.Conditionf(t, comp, msg, args...)
}

// DirExists checks whether a directory exists in the given path. It also
// fails if the path is a file rather a directory or there is an error checking
// whether it exists.
func DirExists(t TestingT, path string, msgAndArgs ...any) {
	require.DirExists(t, path, msgAndArgs...)
}

// DirExistsf checks whether a directory exists in the given path. It also
// fails if the path is a file rather a directory or there is an error checking
// whether it exists.
func DirExistsf(t TestingT, path string, msg string, args ...any) {
	require.DirExistsf(t, path, msg, args...)
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
//	require.Empty(t, obj)
func Empty(t TestingT, object any, msgAndArgs ...any) {
	require.Empty(t, object, msgAndArgs...)
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
//	require.Emptyf(t, obj, "error message %s", "formatted")
func Emptyf(t TestingT, object any, msg string, args ...any) {
	require.Emptyf(t, object, msg, args...)
}

// EqualError asserts that a function returned an error (i.e. not `nil`) and
// that it is equal to the provided error.
//
//	actualObj, err := SomeFunction()
//	require.EqualError(t, err,  expectedErrorString)
func EqualError(t TestingT, theError error, errString string, msgAndArgs ...any) {
	require.EqualError(t, theError, errString, msgAndArgs...)
}

// EqualErrorf asserts that a function returned an error (i.e. not `nil`) and
// that it is equal to the provided error.
//
//	actualObj, err := SomeFunction()
//	require.EqualErrorf(t, err,  expectedErrorString, "error message %s", "formatted")
func EqualErrorf(t TestingT, theError error, errString string, msg string, args ...any) {
	require.EqualErrorf(t, theError, errString, msg, args...)
}

// Error asserts that a function returned an error (i.e. not `nil`).
//
//	actualObj, err := SomeFunction()
//	require.Error(t, err)
func Error(t TestingT, err error, msgAndArgs ...any) {
	require.Error(t, err, msgAndArgs...)
}

// ErrorAs asserts that at least one of the errors in err's chain matches
// target, and if so, sets target to that error value. This is a wrapper for
// errors.As.
func ErrorAs(t TestingT, err error, target any, msgAndArgs ...any) {
	require.ErrorAs(t, err, target, msgAndArgs...)
}

// ErrorAsf asserts that at least one of the errors in err's chain matches
// target, and if so, sets target to that error value. This is a wrapper for
// errors.As.
func ErrorAsf(t TestingT, err error, target any, msg string, args ...any) {
	require.ErrorAsf(t, err, target, msg, args...)
}

// ErrorContains asserts that a function returned an error (i.e. not `nil`) and
// that the error contains the specified substring.
//
//	actualObj, err := SomeFunction()
//	require.ErrorContains(t, err,  expectedErrorSubString)
func ErrorContains(t TestingT, theError error, contains string, msgAndArgs ...any) {
	require.ErrorContains(t, theError, contains, msgAndArgs...)
}

// ErrorContainsf asserts that a function returned an error (i.e. not `nil`)
// and that the error contains the specified substring.
//
//	actualObj, err := SomeFunction()
//	require.ErrorContainsf(t, err,  expectedErrorSubString, "error message %s", "formatted")
func ErrorContainsf(t TestingT, theError error, contains string, msg string, args ...any) {
	require.ErrorContainsf(t, theError, contains, msg, args...)
}

// ErrorIs asserts that at least one of the errors in err's chain matches
// target. This is a wrapper for errors.Is.
func ErrorIs(t TestingT, err error, target error, msgAndArgs ...any) {
	require.ErrorIs(t, err, target, msgAndArgs...)
}

// ErrorIsf asserts that at least one of the errors in err's chain matches
// target. This is a wrapper for errors.Is.
func ErrorIsf(t TestingT, err error, target error, msg string, args ...any) {
	require.ErrorIsf(t, err, target, msg, args...)
}

// Errorf asserts that a function returned an error (i.e. not `nil`).
//
//	actualObj, err := SomeFunction()
//	require.Errorf(t, err, "error message %s", "formatted")
func Errorf(t TestingT, err error, msg string, args ...any) {
	require.Errorf(t, err, msg, args...)
}

// Eventually asserts that given condition will be met in waitFor time,
// periodically checking target function each tick.
//
//	require.Eventually(t, func() bool { return true; }, time.Second, 10*time.Millisecond)
func Eventually(t TestingT, condition func() bool, waitFor time.Duration, tick time.Duration, msgAndArgs ...any) {
	require.Eventually(t, condition, waitFor, tick, msgAndArgs...)
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
//	require.EventuallyWithT(t, func(c *require.CollectT) {
//		// add assertions as needed; any assertion failure will fail the current tick
//		require.True(c, externalValue, "expected 'externalValue' to be true")
//	}, 10*time.Second, 1*time.Second, "external state has not changed to 'true'; still false")
func EventuallyWithT(t TestingT, condition func(collect *assert.CollectT), waitFor time.Duration, tick time.Duration, msgAndArgs ...any) {
	require.EventuallyWithT(t, condition, waitFor, tick, msgAndArgs...)
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
//	require.EventuallyWithTf(t, func(c *require.CollectT, "error message %s", "formatted") {
//		// add assertions as needed; any assertion failure will fail the current tick
//		require.True(c, externalValue, "expected 'externalValue' to be true")
//	}, 10*time.Second, 1*time.Second, "external state has not changed to 'true'; still false")
func EventuallyWithTf(t TestingT, condition func(collect *assert.CollectT), waitFor time.Duration, tick time.Duration, msg string, args ...any) {
	require.EventuallyWithTf(t, condition, waitFor, tick, msg, args...)
}

// Eventuallyf asserts that given condition will be met in waitFor time,
// periodically checking target function each tick.
//
//	require.Eventuallyf(t, func() bool { return true; }, time.Second, 10*time.Millisecond, "error message %s", "formatted")
func Eventuallyf(t TestingT, condition func() bool, waitFor time.Duration, tick time.Duration, msg string, args ...any) {
	require.Eventuallyf(t, condition, waitFor, tick, msg, args...)
}

// Fail reports a failure through
func Fail(t TestingT, failureMessage string, msgAndArgs ...any) {
	require.Fail(t, failureMessage, msgAndArgs...)
}

// FailNow fails test
func FailNow(t TestingT, failureMessage string, msgAndArgs ...any) {
	require.FailNow(t, failureMessage, msgAndArgs...)
}

// FailNowf fails test
func FailNowf(t TestingT, failureMessage string, msg string, args ...any) {
	require.FailNowf(t, failureMessage, msg, args...)
}

// Failf reports a failure through
func Failf(t TestingT, failureMessage string, msg string, args ...any) {
	require.Failf(t, failureMessage, msg, args...)
}

// False asserts that the specified value is false.
//
//	require.False(t, myBool)
func False(t TestingT, value bool, msgAndArgs ...any) {
	require.False(t, value, msgAndArgs...)
}

// Falsef asserts that the specified value is false.
//
//	require.Falsef(t, myBool, "error message %s", "formatted")
func Falsef(t TestingT, value bool, msg string, args ...any) {
	require.Falsef(t, value, msg, args...)
}

// FileExists checks whether a file exists in the given path. It also fails if
// the path points to a directory or there is an error when trying to check the
// file.
func FileExists(t TestingT, path string, msgAndArgs ...any) {
	require.FileExists(t, path, msgAndArgs...)
}

// FileExistsf checks whether a file exists in the given path. It also fails if
// the path points to a directory or there is an error when trying to check the
// file.
func FileExistsf(t TestingT, path string, msg string, args ...any) {
	require.FileExistsf(t, path, msg, args...)
}

// HTTPBodyContains asserts that a specified handler returns a body that
// contains a string.
//
//	require.HTTPBodyContains(t, myHandler, "GET", "www.google.com", nil, "I'm Feeling Lucky")
//
// Returns whether the assertion was successful (true) or not (false).
func HTTPBodyContains(t TestingT, handler http.HandlerFunc, method, url string, values url.Values, str string, msgAndArgs ...any) {
	require.HTTPBodyContains(t, handler, method, url, values, str, msgAndArgs...)
}

// HTTPBodyContainsf asserts that a specified handler returns a body that
// contains a string.
//
//	require.HTTPBodyContainsf(t, myHandler, "GET", "www.google.com", nil, "I'm Feeling Lucky", "error message %s", "formatted")
//
// Returns whether the assertion was successful (true) or not (false).
func HTTPBodyContainsf(t TestingT, handler http.HandlerFunc, method string, url string, values url.Values, str string, msg string, args ...any) {
	require.HTTPBodyContainsf(t, handler, method, url, values, str, msg, args...)
}

// HTTPBodyNotContains asserts that a specified handler returns a body that
// does not contain a string.
//
//	require.HTTPBodyNotContains(t, myHandler, "GET", "www.google.com", nil, "I'm Feeling Lucky")
//
// Returns whether the assertion was successful (true) or not (false).
func HTTPBodyNotContains(t TestingT, handler http.HandlerFunc, method, url string, values url.Values, str string, msgAndArgs ...any) {
	require.HTTPBodyNotContains(t, handler, method, url, values, str, msgAndArgs...)
}

// HTTPBodyNotContainsf asserts that a specified handler returns a body that
// does not contain a string.
//
//	require.HTTPBodyNotContainsf(t, myHandler, "GET", "www.google.com", nil, "I'm Feeling Lucky", "error message %s", "formatted")
//
// Returns whether the assertion was successful (true) or not (false).
func HTTPBodyNotContainsf(t TestingT, handler http.HandlerFunc, method string, url string, values url.Values, str string, msg string, args ...any) {
	require.HTTPBodyNotContainsf(t, handler, method, url, values, str, msg, args...)
}

// HTTPError asserts that a specified handler returns an error status code.
//
//	require.HTTPError(t, myHandler, "POST", "/a/b/c", url.Values{"a": []string{"b", "c"}}
//
// Returns whether the assertion was successful (true) or not (false).
func HTTPError(t TestingT, handler http.HandlerFunc, method, url string, values url.Values, msgAndArgs ...any) {
	require.HTTPError(t, handler, method, url, values, msgAndArgs...)
}

// HTTPErrorf asserts that a specified handler returns an error status code.
//
//	require.HTTPErrorf(t, myHandler, "POST", "/a/b/c", url.Values{"a": []string{"b", "c"}}
//
// Returns whether the assertion was successful (true) or not (false).
func HTTPErrorf(t TestingT, handler http.HandlerFunc, method string, url string, values url.Values, msg string, args ...any) {
	require.HTTPErrorf(t, handler, method, url, values, msg, args...)
}

// HTTPRedirect asserts that a specified handler returns a redirect status
// code.
//
//	require.HTTPRedirect(t, myHandler, "GET", "/a/b/c", url.Values{"a": []string{"b", "c"}}
//
// Returns whether the assertion was successful (true) or not (false).
func HTTPRedirect(t TestingT, handler http.HandlerFunc, method, url string, values url.Values, msgAndArgs ...any) {
	require.HTTPRedirect(t, handler, method, url, values, msgAndArgs...)
}

// HTTPRedirectf asserts that a specified handler returns a redirect status
// code.
//
//	require.HTTPRedirectf(t, myHandler, "GET", "/a/b/c", url.Values{"a": []string{"b", "c"}}
//
// Returns whether the assertion was successful (true) or not (false).
func HTTPRedirectf(t TestingT, handler http.HandlerFunc, method string, url string, values url.Values, msg string, args ...any) {
	require.HTTPRedirectf(t, handler, method, url, values, msg, args...)
}

// HTTPStatusCode asserts that a specified handler returns a specified status
// code.
//
//	require.HTTPStatusCode(t, myHandler, "GET", "/notImplemented", nil, 501)
//
// Returns whether the assertion was successful (true) or not (false).
func HTTPStatusCode(t TestingT, handler http.HandlerFunc, method, url string, values url.Values, expectedStatusCode int, msgAndArgs ...any) {
	require.HTTPStatusCode(t, handler, method, url, values, expectedStatusCode, msgAndArgs...)
}

// HTTPStatusCodef asserts that a specified handler returns a specified status
// code.
//
//	require.HTTPStatusCodef(t, myHandler, "GET", "/notImplemented", nil, 501, "error message %s", "formatted")
//
// Returns whether the assertion was successful (true) or not (false).
func HTTPStatusCodef(t TestingT, handler http.HandlerFunc, method string, url string, values url.Values, expectedStatusCode int, msg string, args ...any) {
	require.HTTPStatusCodef(t, handler, method, url, values, expectedStatusCode, msg, args...)
}

// HTTPSuccess asserts that a specified handler returns a success status code.
//
//	require.HTTPSuccess(t, myHandler, "POST", "http://www.google.com", nil)
//
// Returns whether the assertion was successful (true) or not (false).
func HTTPSuccess(t TestingT, handler http.HandlerFunc, method, url string, values url.Values, msgAndArgs ...any) {
	require.HTTPSuccess(t, handler, method, url, values, msgAndArgs...)
}

// HTTPSuccessf asserts that a specified handler returns a success status code.
//
//	require.HTTPSuccessf(t, myHandler, "POST", "http://www.google.com", nil, "error message %s", "formatted")
//
// Returns whether the assertion was successful (true) or not (false).
func HTTPSuccessf(t TestingT, handler http.HandlerFunc, method string, url string, values url.Values, msg string, args ...any) {
	require.HTTPSuccessf(t, handler, method, url, values, msg, args...)
}

// Implements asserts that an object is implemented by the specified interface.
//
//	require.Implements(t, (*MyInterface)(nil), new(MyObject))
func Implements(t TestingT, interfaceObject any, object any, msgAndArgs ...any) {
	require.Implements(t, interfaceObject, object, msgAndArgs...)
}

// Implementsf asserts that an object is implemented by the specified
// interface.
//
//	require.Implementsf(t, (*MyInterface)(nil), new(MyObject), "error message %s", "formatted")
func Implementsf(t TestingT, interfaceObject any, object any, msg string, args ...any) {
	require.Implementsf(t, interfaceObject, object, msg, args...)
}

// IsDecreasing asserts that the collection is decreasing
//
//	require.IsDecreasing(t, []int{2, 1, 0})
//	require.IsDecreasing(t, []float{2, 1})
//	require.IsDecreasing(t, []string{"b", "a"})
func IsDecreasing(t TestingT, object any, msgAndArgs ...any) {
	require.IsDecreasing(t, object, msgAndArgs...)
}

// IsDecreasingf asserts that the collection is decreasing
//
//	require.IsDecreasingf(t, []int{2, 1, 0}, "error message %s", "formatted")
//	require.IsDecreasingf(t, []float{2, 1}, "error message %s", "formatted")
//	require.IsDecreasingf(t, []string{"b", "a"}, "error message %s", "formatted")
func IsDecreasingf(t TestingT, object any, msg string, args ...any) {
	require.IsDecreasingf(t, object, msg, args...)
}

// IsIncreasing asserts that the collection is increasing
//
//	require.IsIncreasing(t, []int{1, 2, 3})
//	require.IsIncreasing(t, []float{1, 2})
//	require.IsIncreasing(t, []string{"a", "b"})
func IsIncreasing(t TestingT, object any, msgAndArgs ...any) {
	require.IsIncreasing(t, object, msgAndArgs...)
}

// IsIncreasingf asserts that the collection is increasing
//
//	require.IsIncreasingf(t, []int{1, 2, 3}, "error message %s", "formatted")
//	require.IsIncreasingf(t, []float{1, 2}, "error message %s", "formatted")
//	require.IsIncreasingf(t, []string{"a", "b"}, "error message %s", "formatted")
func IsIncreasingf(t TestingT, object any, msg string, args ...any) {
	require.IsIncreasingf(t, object, msg, args...)
}

// IsNonDecreasing asserts that the collection is not decreasing
//
//	require.IsNonDecreasing(t, []int{1, 1, 2})
//	require.IsNonDecreasing(t, []float{1, 2})
//	require.IsNonDecreasing(t, []string{"a", "b"})
func IsNonDecreasing(t TestingT, object any, msgAndArgs ...any) {
	require.IsNonDecreasing(t, object, msgAndArgs...)
}

// IsNonDecreasingf asserts that the collection is not decreasing
//
//	require.IsNonDecreasingf(t, []int{1, 1, 2}, "error message %s", "formatted")
//	require.IsNonDecreasingf(t, []float{1, 2}, "error message %s", "formatted")
//	require.IsNonDecreasingf(t, []string{"a", "b"}, "error message %s", "formatted")
func IsNonDecreasingf(t TestingT, object any, msg string, args ...any) {
	require.IsNonDecreasingf(t, object, msg, args...)
}

// IsNonIncreasing asserts that the collection is not increasing
//
//	require.IsNonIncreasing(t, []int{2, 1, 1})
//	require.IsNonIncreasing(t, []float{2, 1})
//	require.IsNonIncreasing(t, []string{"b", "a"})
func IsNonIncreasing(t TestingT, object any, msgAndArgs ...any) {
	require.IsNonIncreasing(t, object, msgAndArgs...)
}

// IsNonIncreasingf asserts that the collection is not increasing
//
//	require.IsNonIncreasingf(t, []int{2, 1, 1}, "error message %s", "formatted")
//	require.IsNonIncreasingf(t, []float{2, 1}, "error message %s", "formatted")
//	require.IsNonIncreasingf(t, []string{"b", "a"}, "error message %s", "formatted")
func IsNonIncreasingf(t TestingT, object any, msg string, args ...any) {
	require.IsNonIncreasingf(t, object, msg, args...)
}

// JSONEq asserts that two JSON strings are equivalent.
//
//	require.JSONEq(t, `{"hello": "world", "foo": "bar"}`, `{"foo": "bar", "hello": "world"}`)
func JSONEq(t TestingT, expected string, actual string, msgAndArgs ...any) {
	require.JSONEq(t, expected, actual, msgAndArgs...)
}

// JSONEqf asserts that two JSON strings are equivalent.
//
//	require.JSONEqf(t, `{"hello": "world", "foo": "bar"}`, `{"foo": "bar", "hello": "world"}`, "error message %s", "formatted")
func JSONEqf(t TestingT, expected string, actual string, msg string, args ...any) {
	require.JSONEqf(t, expected, actual, msg, args...)
}

// Len asserts that the specified object has specific length. Len also fails if
// the object has a type that len() not accept.
//
//	require.Len(t, mySlice, 3)
func Len(t TestingT, object any, length int, msgAndArgs ...any) {
	require.Len(t, object, length, msgAndArgs...)
}

// Lenf asserts that the specified object has specific length. Lenf also fails
// if the object has a type that len() not accept.
//
//	require.Lenf(t, mySlice, 3, "error message %s", "formatted")
func Lenf(t TestingT, object any, length int, msg string, args ...any) {
	require.Lenf(t, object, length, msg, args...)
}

// Negative asserts that the specified element is negative
//
//	require.Negative(t, -1)
//	require.Negative(t, -1.23)
func Negative(t TestingT, e any, msgAndArgs ...any) {
	require.Negative(t, e, msgAndArgs...)
}

// Negativef asserts that the specified element is negative
//
//	require.Negativef(t, -1, "error message %s", "formatted")
//	require.Negativef(t, -1.23, "error message %s", "formatted")
func Negativef(t TestingT, e any, msg string, args ...any) {
	require.Negativef(t, e, msg, args...)
}

// Never asserts that the given condition doesn't satisfy in waitFor time,
// periodically checking the target function each tick.
//
//	require.Never(t, func() bool { return false; }, time.Second, 10*time.Millisecond)
func Never(t TestingT, condition func() bool, waitFor time.Duration, tick time.Duration, msgAndArgs ...any) {
	require.Never(t, condition, waitFor, tick, msgAndArgs...)
}

// Neverf asserts that the given condition doesn't satisfy in waitFor time,
// periodically checking the target function each tick.
//
//	require.Neverf(t, func() bool { return false; }, time.Second, 10*time.Millisecond, "error message %s", "formatted")
func Neverf(t TestingT, condition func() bool, waitFor time.Duration, tick time.Duration, msg string, args ...any) {
	require.Neverf(t, condition, waitFor, tick, msg, args...)
}

// Nil asserts that the specified object is nil.
//
//	require.Nil(t, err)
func Nil(t TestingT, object any, msgAndArgs ...any) {
	require.Nil(t, object, msgAndArgs...)
}

// Nilf asserts that the specified object is nil.
//
//	require.Nilf(t, err, "error message %s", "formatted")
func Nilf(t TestingT, object any, msg string, args ...any) {
	require.Nilf(t, object, msg, args...)
}

// NoDirExists checks whether a directory does not exist in the given path.
// It fails if the path points to an existing _directory_ only.
func NoDirExists(t TestingT, path string, msgAndArgs ...any) {
	require.NoDirExists(t, path, msgAndArgs...)
}

// NoDirExistsf checks whether a directory does not exist in the given path.
// It fails if the path points to an existing _directory_ only.
func NoDirExistsf(t TestingT, path string, msg string, args ...any) {
	require.NoDirExistsf(t, path, msg, args...)
}

// NoError asserts that a function returned no error (i.e. `nil`).
//
//	  actualObj, err := SomeFunction()
//	  if require.NoError(t, err) {
//		   require.Equal(t, expectedObj, actualObj)
//	  }
func NoError(t TestingT, err error, msgAndArgs ...any) {
	require.NoError(t, err, msgAndArgs...)
}

// NoErrorf asserts that a function returned no error (i.e. `nil`).
//
//	  actualObj, err := SomeFunction()
//	  if require.NoErrorf(t, err, "error message %s", "formatted") {
//		   require.Equal(t, expectedObj, actualObj)
//	  }
func NoErrorf(t TestingT, err error, msg string, args ...any) {
	require.NoErrorf(t, err, msg, args...)
}

// NoFileExists checks whether a file does not exist in a given path. It fails
// if the path points to an existing _file_ only.
func NoFileExists(t TestingT, path string, msgAndArgs ...any) {
	require.NoFileExists(t, path, msgAndArgs...)
}

// NoFileExistsf checks whether a file does not exist in a given path. It fails
// if the path points to an existing _file_ only.
func NoFileExistsf(t TestingT, path string, msg string, args ...any) {
	require.NoFileExistsf(t, path, msg, args...)
}

// NotEmpty asserts that the specified object is NOT Empty.
//
//	if require.NotEmpty(t, obj) {
//	  require.Equal(t, "two", obj[1])
//	}
func NotEmpty(t TestingT, object any, msgAndArgs ...any) {
	require.NotEmpty(t, object, msgAndArgs...)
}

// NotEmptyf asserts that the specified object is NOT Empty.
//
//	if require.NotEmptyf(t, obj, "error message %s", "formatted") {
//	  require.Equal(t, "two", obj[1])
//	}
func NotEmptyf(t TestingT, object any, msg string, args ...any) {
	require.NotEmptyf(t, object, msg, args...)
}

// NotErrorAs asserts that none of the errors in err's chain matches target,
// but if so, sets target to that error value.
func NotErrorAs(t TestingT, err error, target any, msgAndArgs ...any) {
	require.NotErrorAs(t, err, target, msgAndArgs...)
}

// NotErrorAsf asserts that none of the errors in err's chain matches target,
// but if so, sets target to that error value.
func NotErrorAsf(t TestingT, err error, target any, msg string, args ...any) {
	require.NotErrorAsf(t, err, target, msg, args...)
}

// NotErrorIs asserts that none of the errors in err's chain matches target.
// This is a wrapper for errors.Is.
func NotErrorIs(t TestingT, err error, target error, msgAndArgs ...any) {
	require.NotErrorIs(t, err, target, msgAndArgs...)
}

// NotErrorIsf asserts that none of the errors in err's chain matches target.
// This is a wrapper for errors.Is.
func NotErrorIsf(t TestingT, err error, target error, msg string, args ...any) {
	require.NotErrorIsf(t, err, target, msg, args...)
}

// NotNil asserts that the specified object is not nil.
//
//	require.NotNil(t, err)
func NotNil(t TestingT, object any, msgAndArgs ...any) {
	require.NotNil(t, object, msgAndArgs...)
}

// NotNilf asserts that the specified object is not nil.
//
//	require.NotNilf(t, err, "error message %s", "formatted")
func NotNilf(t TestingT, object any, msg string, args ...any) {
	require.NotNilf(t, object, msg, args...)
}

// NotPanics asserts that the code inside the specified PanicTestFunc does NOT
// panic.
//
//	require.NotPanics(t, func(){ RemainCalm() })
func NotPanics(t TestingT, f assert.PanicTestFunc, msgAndArgs ...any) {
	require.NotPanics(t, f, msgAndArgs...)
}

// NotPanicsf asserts that the code inside the specified PanicTestFunc does NOT
// panic.
//
//	require.NotPanicsf(t, func(){ RemainCalm() }, "error message %s", "formatted")
func NotPanicsf(t TestingT, f assert.PanicTestFunc, msg string, args ...any) {
	require.NotPanicsf(t, f, msg, args...)
}

// NotZero asserts that i is not the zero value for its type.
func NotZero(t TestingT, i any, msgAndArgs ...any) {
	require.NotZero(t, i, msgAndArgs...)
}

// NotZerof asserts that i is not the zero value for its type.
func NotZerof(t TestingT, i any, msg string, args ...any) {
	require.NotZerof(t, i, msg, args...)
}

// Panics asserts that the code inside the specified PanicTestFunc panics.
//
//	require.Panics(t, func(){ GoCrazy() })
func Panics(t TestingT, f assert.PanicTestFunc, msgAndArgs ...any) {
	require.Panics(t, f, msgAndArgs...)
}

// PanicsWithError asserts that the code inside the specified PanicTestFunc
// panics, and that the recovered panic value is an error that satisfies the
// EqualError comparison.
//
//	require.PanicsWithError(t, "crazy error", func(){ GoCrazy() })
func PanicsWithError(t TestingT, errString string, f assert.PanicTestFunc, msgAndArgs ...any) {
	require.PanicsWithError(t, errString, f, msgAndArgs...)
}

// PanicsWithErrorf asserts that the code inside the specified PanicTestFunc
// panics, and that the recovered panic value is an error that satisfies the
// EqualError comparison.
//
//	require.PanicsWithErrorf(t, "crazy error", func(){ GoCrazy() }, "error message %s", "formatted")
func PanicsWithErrorf(t TestingT, errString string, f assert.PanicTestFunc, msg string, args ...any) {
	require.PanicsWithErrorf(t, errString, f, msg, args...)
}

// PanicsWithValue asserts that the code inside the specified PanicTestFunc
// panics, and that the recovered panic value equals the expected panic value.
//
//	require.PanicsWithValue(t, "crazy error", func(){ GoCrazy() })
func PanicsWithValue(t TestingT, expected any, f assert.PanicTestFunc, msgAndArgs ...any) {
	require.PanicsWithValue(t, expected, f, msgAndArgs...)
}

// PanicsWithValuef asserts that the code inside the specified PanicTestFunc
// panics, and that the recovered panic value equals the expected panic value.
//
//	require.PanicsWithValuef(t, "crazy error", func(){ GoCrazy() }, "error message %s", "formatted")
func PanicsWithValuef(t TestingT, expected any, f assert.PanicTestFunc, msg string, args ...any) {
	require.PanicsWithValuef(t, expected, f, msg, args...)
}

// Panicsf asserts that the code inside the specified PanicTestFunc panics.
//
//	require.Panicsf(t, func(){ GoCrazy() }, "error message %s", "formatted")
func Panicsf(t TestingT, f assert.PanicTestFunc, msg string, args ...any) {
	require.Panicsf(t, f, msg, args...)
}

// Positive asserts that the specified element is positive
//
//	require.Positive(t, 1)
//	require.Positive(t, 1.23)
func Positive(t TestingT, e any, msgAndArgs ...any) {
	require.Positive(t, e, msgAndArgs...)
}

// Positivef asserts that the specified element is positive
//
//	require.Positivef(t, 1, "error message %s", "formatted")
//	require.Positivef(t, 1.23, "error message %s", "formatted")
func Positivef(t TestingT, e any, msg string, args ...any) {
	require.Positivef(t, e, msg, args...)
}

// True asserts that the specified value is true.
//
//	require.True(t, myBool)
func True(t TestingT, value bool, msgAndArgs ...any) {
	require.True(t, value, msgAndArgs...)
}

// Truef asserts that the specified value is true.
//
//	require.Truef(t, myBool, "error message %s", "formatted")
func Truef(t TestingT, value bool, msg string, args ...any) {
	require.Truef(t, value, msg, args...)
}

// WithinDuration asserts that the two times are within duration delta of each
// other.
//
//	require.WithinDuration(t, time.Now(), time.Now(), 10*time.Second)
func WithinDuration(t TestingT, expected time.Time, actual time.Time, delta time.Duration, msgAndArgs ...any) {
	require.WithinDuration(t, expected, actual, delta, msgAndArgs...)
}

// WithinDurationf asserts that the two times are within duration delta of each
// other.
//
//	require.WithinDurationf(t, time.Now(), time.Now(), 10*time.Second, "error message %s", "formatted")
func WithinDurationf(t TestingT, expected time.Time, actual time.Time, delta time.Duration, msg string, args ...any) {
	require.WithinDurationf(t, expected, actual, delta, msg, args...)
}

// WithinRange asserts that a time is within a time range (inclusive).
//
//	require.WithinRange(t, time.Now(), time.Now().Add(-time.Second), time.Now().Add(time.Second))
func WithinRange(t TestingT, actual time.Time, start time.Time, end time.Time, msgAndArgs ...any) {
	require.WithinRange(t, actual, start, end, msgAndArgs...)
}

// WithinRangef asserts that a time is within a time range (inclusive).
//
//	require.WithinRangef(t, time.Now(), time.Now().Add(-time.Second), time.Now().Add(time.Second), "error message %s", "formatted")
func WithinRangef(t TestingT, actual time.Time, start time.Time, end time.Time, msg string, args ...any) {
	require.WithinRangef(t, actual, start, end, msg, args...)
}

// YAMLEq asserts that two YAML strings are equivalent.
func YAMLEq(t TestingT, expected string, actual string, msgAndArgs ...any) {
	require.YAMLEq(t, expected, actual, msgAndArgs...)
}

// YAMLEqf asserts that two YAML strings are equivalent.
func YAMLEqf(t TestingT, expected string, actual string, msg string, args ...any) {
	require.YAMLEqf(t, expected, actual, msg, args...)
}

// Zero asserts that i is the zero value for its type.
func Zero(t TestingT, i any, msgAndArgs ...any) {
	require.Zero(t, i, msgAndArgs...)
}

// Zerof asserts that i is the zero value for its type.
func Zerof(t TestingT, i any, msg string, args ...any) {
	require.Zerof(t, i, msg, args...)
}
