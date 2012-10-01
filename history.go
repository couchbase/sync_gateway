// history.go

package couchglue

import (
    "encoding/json"
)


/*  A revision map is stored in the _revmap property of a document.
    It's a flat structure that maps each revision ID to its parent's ID.
    Root revisions have a parent of "" (the empty string). */
type RevMap map[string]string


// The form in which a RevMap is stored in JSON.
type RevTreeList struct {
    Revs    []string    `json:"revs"`
    Parents []int       `json:"parents"`
}


func (revmap RevMap) MarshalJSON() ([]byte, error) {
    revs := make([]string, 0, len(revmap))
    parents := make([]int, 0, len(revmap))
    revIndexes := map[string]int {"": -1}
    
    for rev,_:= range(revmap) {
        revIndexes[rev] = len(revs)
        revs = append(revs, rev)
    }
    for _,rev := range(revs) {
        parent := revmap[rev]
        parents = append(parents, revIndexes[parent])
    }
    
    return json.Marshal(RevTreeList{Revs: revs, Parents: parents})
}


func (revmap RevMap) UnmarshalJSON(inputjson []byte) (err error ){
    var rep RevTreeList
    err = json.Unmarshal(inputjson, &rep)
    if err != nil { return }
        
    for i,rev:= range(rep.Revs) {
        parentIndex := rep.Parents[i]
        if parentIndex >= 0 {
            revmap[rev] = rep.Revs[parentIndex]
        } else {
            revmap[rev] = ""
        }
    }
    return
}


// Returns true if the RevMap has an entry for this revid.
func (revmap RevMap) contains(revid string) bool {
    _, exists := revmap[revid]
    return exists
}


// Returns the parent ID of a revid. The parent is "" if the revid is a root.
// Panics if the revid is not in the map at all.
func (revmap RevMap) getParent(revid string) string {
    parent, exists := revmap[revid]
    if !exists {panic("can't find parent")}
    return parent
}


// Returns the history of a revid as an array of revids in reverse chronological order.
func (revmap RevMap) getHistory(revid string) []string {
    history := make([]string, 0, 5)
    for revid != "" {
        history = append(history, revid)
        revid = revmap.getParent(revid)
    }
    return history
}


// Returns the leaf revision IDs (those that have no children.)
func (revmap RevMap) getLeaves() []string {
    isParent := map[string]bool{}
    for _, parent := range(revmap) {
        isParent[parent] = true
    }
    leaves := make([]string, 0, len(revmap) - len(isParent) + 1)
    for revid, _ := range(revmap) {
        if !isParent[revid] {
            leaves = append(leaves, revid)
        }
    }
    return leaves
}


// Records a revision in a RevMap.
func (revmap RevMap) addRevision(revid string, parentid string) {
    if revid == "" { panic("empty revid is illegal") }
    if parentid != "" && !revmap.contains(parentid) { panic("parent id is missing") }
    if revmap.contains(revid) { panic("already contains this rev") }
    revmap[revid] = parentid
}


func (revmap RevMap) copy() RevMap {
    result := RevMap{}
    for rev, parent := range(revmap) {
        result[rev] = parent
    }
    return result
}