// revtree.go

package couchglue

import (
    "encoding/json"
    "fmt"
)


/*  A revision tree maps each revision ID to its parent's ID.
    Root revisions have a parent of "" (the empty string). */
type RevTree map[string]string


// The form in which a RevTree is stored in JSON. For space-efficiency it's stored as an array of
// rev IDs, with a parallel array of parent indexes. Ordering in the arrays doesn't matter.
// So the parent of Revs[i] is Revs[Parents[i]] (unless Parents[i] == -1, which denotes a root.)
type RevTreeList struct {
    Revs    []string    `json:"revs"`
    Parents []int       `json:"parents"`
}


func (tree RevTree) MarshalJSON() ([]byte, error) {
    revs := make([]string, 0, len(tree))
    parents := make([]int, 0, len(tree))
    revIndexes := map[string]int {"": -1}
    
    for rev,_:= range(tree) {
        revIndexes[rev] = len(revs)
        revs = append(revs, rev)
    }
    for _,rev := range(revs) {
        parent := tree[rev]
        parents = append(parents, revIndexes[parent])
    }
    
    return json.Marshal(RevTreeList{Revs: revs, Parents: parents})
}


func (tree RevTree) UnmarshalJSON(inputjson []byte) (err error ){
    var rep RevTreeList
    err = json.Unmarshal(inputjson, &rep)
    if err != nil { return }
        
    for i,rev:= range(rep.Revs) {
        parentIndex := rep.Parents[i]
        if parentIndex >= 0 {
            tree[rev] = rep.Revs[parentIndex]
        } else {
            tree[rev] = ""
        }
    }
    return
}


// Returns true if the RevTree has an entry for this revid.
func (tree RevTree) contains(revid string) bool {
    _, exists := tree[revid]
    return exists
}


// Returns the parent ID of a revid. The parent is "" if the revid is a root.
// Panics if the revid is not in the map at all.
func (tree RevTree) getParent(revid string) string {
    parent, exists := tree[revid]
    if !exists {panic("can't find parent")}
    return parent
}


// Returns the history of a revid as an array of revids in reverse chronological order.
func (tree RevTree) getHistory(revid string) []string {
    history := make([]string, 0, 5)
    for revid != "" {
        history = append(history, revid)
        revid = tree.getParent(revid)
    }
    return history
}


// Returns the leaf revision IDs (those that have no children.)
func (tree RevTree) getLeaves() []string {
    isParent := map[string]bool{}
    for _, parent := range(tree) {
        isParent[parent] = true
    }
    leaves := make([]string, 0, len(tree) - len(isParent) + 1)
    for revid, _ := range(tree) {
        if !isParent[revid] {
            leaves = append(leaves, revid)
        }
    }
    return leaves
}


// Records a revision in a RevTree.
func (tree RevTree) addRevision(revid string, parentid string) {
    if revid == "" { panic("empty revid is illegal") }
    if parentid != "" && !tree.contains(parentid) {
        panic(fmt.Sprintf("parent id %q is missing", parentid))
    }
    if tree.contains(revid) {
        panic(fmt.Sprintf("already contains rev %q", revid))
    }
    tree[revid] = parentid
}


// Deep-copies a RevTree.
func (tree RevTree) copy() RevTree {
    result := RevTree{}
    for rev, parent := range(tree) {
        result[rev] = parent
    }
    return result
}