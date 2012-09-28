// history_test.go

package couchglue

import (
    "reflect"
    "sort"
    "testing"
)

var testmap = RevMap{"3-three": "2-two", "2-two": "1-one", "1-one": ""}
var testbody = Body{"_id": "foo", "_rev": "3-three", "_revmap": testmap}
var branchymap = RevMap{"3-three": "2-two", "2-two": "1-one", "1-one": "",
                        "3-drei": "2-zwei", "2-zwei": "1-one"}

func TestBodyAccess(t *testing.T) {
    gotmap := testbody.revMap()
    if !reflect.DeepEqual(gotmap, testmap) {
        t.Errorf("revMap() accessor failed: gotmap=%v, testmap=%v", gotmap, testmap)
    }

    nobody := Body{"_id": "foo", "_rev": "3-three"}
    if nobody.revMap() != nil {
        t.Errorf("revMap() false positive")
    }
}

func TestRevMapAccess(t *testing.T) {
    if !testmap.contains("3-three") {
        t.Errorf("contains 3 failed")
    }
    if !testmap.contains("1-one") {
        t.Errorf("contains 1 failed")
    }
    if testmap.contains("foo") {
        t.Errorf("contains false positive")
    }
}

func TestParentAccess(t *testing.T) {
    parent := testmap.getParent("3-three")
    if parent != "2-two" {
        t.Errorf("parent of 3 failed")
    }
    parent = testmap.getParent("1-one")
    if parent != "" {
        t.Errorf("parent of 1 failed")
    }
}

func TestGetHistory(t *testing.T) {
    history := testmap.getHistory("3-three")
    if !reflect.DeepEqual(history, []string{"3-three", "2-two", "1-one"}) {
        t.Errorf("history of 3 failed")
    }
}

func TestGetLeaves(t *testing.T) {
    leaves := testmap.getLeaves()
    if !reflect.DeepEqual(leaves, []string{"3-three"}) {
        t.Errorf("getLeaves failed")
    }
    leaves = branchymap.getLeaves()
    sort.Strings(leaves)
    if !reflect.DeepEqual(leaves, []string{"3-drei", "3-three"}) {
        t.Errorf("getLeaves failed on branchy")
    }
}


func TestAddRevision(t *testing.T) {
    tempmap := testmap.copy()
    tempmap.addRevision("4-four", "3-three")
    if tempmap.getParent("4-four") != "3-three" {
        t.Errorf("failed to add revision")
    }
}