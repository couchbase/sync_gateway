// history.go

package couchglue


/*  A revision map is stored in the _revmap property of a document.
    It's a flat structure that maps each revision ID to its parent's ID.
    Root revisions have a parent of "" (the empty string). */
type RevMap map[string]string


// Returns the RevMap of a document Body (the value of its "_revmap" property.)
func (body Body) revMap() RevMap {
    revmap := body["_revmap"]
    if revmap == nil {
        return nil
    }
    return revmap.(RevMap)
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