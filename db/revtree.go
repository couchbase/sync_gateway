//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package db

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	"errors"

	"github.com/couchbase/sync_gateway/base"
)

type RevKey string

// Information about a single revision.
type RevInfo struct {
	ID       string
	Parent   string
	Deleted  bool
	Body     []byte
	Channels base.Set
}

//  A revision tree maps each revision ID to its RevInfo.
type RevTree map[string]*RevInfo

// The form in which a RevTree is stored in JSON. For space-efficiency it's stored as an array of
// rev IDs, with a parallel array of parent indexes. Ordering in the arrays doesn't matter.
// So the parent of Revs[i] is Revs[Parents[i]] (unless Parents[i] == -1, which denotes a root.)
type revTreeList struct {
	Revs       []string          `json:"revs"`              // The revision IDs
	Parents    []int             `json:"parents"`           // Index of parent of each revision (-1 if root)
	Deleted    []int             `json:"deleted,omitempty"` // Indexes of revisions that are deletions
	Bodies_Old []string          `json:"bodies,omitempty"`  // JSON of each revision (legacy)
	BodyMap    map[string]string `json:"bodymap,omitempty"` // JSON of each revision
	Channels   []base.Set        `json:"channels"`
}

func (tree RevTree) MarshalJSON() ([]byte, error) {
	n := len(tree)
	rep := revTreeList{
		Revs:     make([]string, n),
		Parents:  make([]int, n),
		Channels: make([]base.Set, n),
	}
	revIndexes := map[string]int{"": -1}

	i := 0
	for _, info := range tree {
		revIndexes[info.ID] = i
		rep.Revs[i] = info.ID
		if info.Body != nil {
			if rep.BodyMap == nil {
				rep.BodyMap = make(map[string]string, 1)
			}
			rep.BodyMap[strconv.FormatInt(int64(i), 10)] = string(info.Body)
		}
		rep.Channels[i] = info.Channels
		if info.Deleted {
			if rep.Deleted == nil {
				rep.Deleted = make([]int, 0, 1)
			}
			rep.Deleted = append(rep.Deleted, i)
		}
		i++
	}

	for i, revid := range rep.Revs {
		rep.Parents[i] = revIndexes[tree[revid].Parent]
	}

	return json.Marshal(rep)
}

func (tree RevTree) UnmarshalJSON(inputjson []byte) (err error) {
	if tree == nil {
		//base.Warn("No RevTree for input %q", inputjson)
		return nil
	}
	var rep revTreeList
	err = json.Unmarshal(inputjson, &rep)
	if err != nil {
		return
	}

	for i, revid := range rep.Revs {
		info := RevInfo{ID: revid}
		if rep.BodyMap != nil {
			if body := rep.BodyMap[strconv.FormatInt(int64(i), 10)]; body != "" {
				info.Body = []byte(body)
			}
		} else if rep.Bodies_Old != nil && len(rep.Bodies_Old[i]) > 0 {
			info.Body = []byte(rep.Bodies_Old[i])
		}
		if rep.Channels != nil {
			info.Channels = rep.Channels[i]
		}
		parentIndex := rep.Parents[i]
		if parentIndex >= 0 {
			info.Parent = rep.Revs[parentIndex]
		}
		tree[revid] = &info
	}
	if rep.Deleted != nil {
		for _, i := range rep.Deleted {
			info := tree[rep.Revs[i]]
			info.Deleted = true //because tree[rep.Revs[i]].Deleted=true is a compile error
			tree[rep.Revs[i]] = info
		}
	}
	return
}

// Returns true if the RevTree has an entry for this revid.
func (tree RevTree) contains(revid string) bool {
	_, exists := tree[revid]
	return exists
}

// Returns the RevInfo for a revision ID, or panics if it's not found
func (tree RevTree) getInfo(revid string) (*RevInfo, error) {
	info, exists := tree[revid]
	if !exists {
		return nil, errors.New("getInfo can't find rev: " + revid)
	}
	return info, nil
}

// Returns the parent ID of a revid. The parent is "" if the revid is a root.
// Panics if the revid is not in the map at all.
func (tree RevTree) getParent(revid string) string {
	info, err := tree.getInfo(revid)
	if err != nil {
		return ""
	}
	return info.Parent
}

// Returns the history of a revid as an array of revids in reverse chronological order.
func (tree RevTree) getHistory(revid string) []string {
	history := make([]string, 0, 5)
	for revid != "" {
		info, err := tree.getInfo(revid)
		if err != nil {
			break
		}
		history = append(history, revid)
		revid = info.Parent
	}
	return history
}

// Returns the leaf revision IDs (those that have no children.)
func (tree RevTree) GetLeaves() []string {
	isParent := map[string]bool{}
	for _, info := range tree {
		isParent[info.Parent] = true
	}
	leaves := make([]string, 0, len(tree)-len(isParent)+1)
	for revid := range tree {
		if !isParent[revid] {
			leaves = append(leaves, revid)
		}
	}
	return leaves
}

func (tree RevTree) forEachLeaf(callback func(*RevInfo)) {
	isParent := map[string]bool{}
	for _, info := range tree {
		isParent[info.Parent] = true
	}
	for revid, info := range tree {
		if !isParent[revid] {
			callback(info)
		}
	}
}

func (tree RevTree) isLeaf(revid string) bool {
	if !tree.contains(revid) {
		return false
	}
	for _, info := range tree {
		if info.Parent == revid {
			return false
		}
	}
	return true
}

// Finds the "winning" revision, the one that should be treated as the default.
// This is the leaf revision whose (!deleted, generation, hash) tuple compares the highest.
func (tree RevTree) winningRevision() (winner string, branched bool, inConflict bool) {
	winnerExists := false
	leafCount := 0
	activeLeafCount := 0
	tree.forEachLeaf(func(info *RevInfo) {
		exists := !info.Deleted
		leafCount++
		if exists {
			activeLeafCount++
		}
		if (exists && !winnerExists) ||
			((exists == winnerExists) && compareRevIDs(info.ID, winner) > 0) {
			winner = info.ID
			winnerExists = exists
		}
	})
	branched = (leafCount > 1)
	inConflict = (activeLeafCount > 1)
	return
}

// Given a revision and a set of possible ancestors, finds the one that is the most recent
// ancestor of the revision; if none are ancestors, returns "".
func (tree RevTree) findAncestorFromSet(revid string, ancestors []string) string {
	//OPT: This is slow...
	for revid != "" {
		for _, a := range ancestors {
			if a == revid {
				return a
			}
		}
		info, err := tree.getInfo(revid)
		if err != nil {
			break
		}
		revid = info.Parent
	}
	return ""
}

// Records a revision in a RevTree.
func (tree RevTree) addRevision(info RevInfo) {
	revid := info.ID
	if revid == "" {
		panic("empty revid is illegal")
	}
	if tree.contains(revid) {
		panic(fmt.Sprintf("already contains rev %q", revid))
	}
	parent := info.Parent
	if parent != "" && !tree.contains(parent) {
		panic(fmt.Sprintf("parent id %q is missing", parent))
	}
	tree[revid] = &info
}

func (tree RevTree) getRevisionBody(revid string) ([]byte, bool) {
	if revid == "" {
		panic("Illegal empty revision ID")
	}
	info, found := tree[revid]
	if !found {
		return nil, false
	}
	return info.Body, true
}

func (tree RevTree) setRevisionBody(revid string, body []byte) {
	if revid == "" {
		panic("Illegal empty revision ID")
	}
	info, found := tree[revid]
	if !found {
		panic(fmt.Sprintf("rev id %q not found", revid))
	}
	info.Body = body
}

func (tree RevTree) getParsedRevisionBody(revid string) Body {
	bodyJSON, found := tree.getRevisionBody(revid)
	if !found || len(bodyJSON) == 0 {
		return nil
	}
	var body Body
	if err := json.Unmarshal(bodyJSON, &body); err != nil {
		panic(fmt.Sprintf("Unexpected error parsing body of rev %q", revid))
	}
	return body
}

// Deep-copies a RevTree.
func (tree RevTree) copy() RevTree {
	result := RevTree{}
	for rev, info := range tree {
		copiedInfo := *info
		result[rev] = &copiedInfo
	}
	return result
}

// Removes older ancestor nodes from the tree; if there are no conflicts, the tree's depth will be
// <= maxDepth. The revision named by `keepRev` will not be pruned (unless `keepRev` is empty.)
// Returns the number of nodes pruned.
func (tree RevTree) pruneRevisions(maxDepth uint32, keepRev string) (pruned int) {
	if len(tree) <= int(maxDepth) {
		return 0
	}

	// Find the minimum generation that has a non-deleted leaf:
	minLeafGen := math.MaxInt32
	maxDeletedLeafGen := 0
	for _, revid := range tree.GetLeaves() {
		gen := genOfRevID(revid)
		if tree[revid].Deleted {
			if gen > maxDeletedLeafGen {
				maxDeletedLeafGen = gen
			}
		} else if gen > 0 && gen < minLeafGen {
			minLeafGen = gen
		}
	}

	if minLeafGen == math.MaxInt32 {
		// If there are no non-deleted leaves, use the deepest leaf's generation
		minLeafGen = maxDeletedLeafGen
	}
	minGenToKeep := int(minLeafGen) - int(maxDepth) + 1

	if gen := genOfRevID(keepRev); gen > 0 && gen < minGenToKeep {
		// Make sure keepRev's generation isn't pruned
		minGenToKeep = gen
	}

	// Delete nodes whose generation is less than minGenToKeep:
	if minGenToKeep > 1 {
		for revid, node := range tree {
			if gen := genOfRevID(revid); gen < minGenToKeep {
				delete(tree, revid)
				pruned++
			} else if gen == minGenToKeep {
				node.Parent = ""
			}
		}
	}
	return
}

//////// ENCODED REVISION LISTS (_revisions):

// Parses a CouchDB _rev or _revisions property into a list of revision IDs
func ParseRevisions(body Body) []string {
	// http://wiki.apache.org/couchdb/HTTP_Document_API#GET
	revisions, ok := body["_revisions"].(map[string]interface{})
	if !ok {
		revid, ok := body["_rev"].(string)
		if !ok {
			return nil
		}
		if genOfRevID(revid) < 1 {
			return nil
		}
		oneRev := make([]string, 0, 1)
		oneRev = append(oneRev, revid)
		return oneRev
	}
	start, ids := splitRevisionList(revisions)
	if ids == nil {
		return nil
	}
	result := make([]string, 0, len(ids))
	for _, id := range ids {
		result = append(result, fmt.Sprintf("%d-%s", start, id))
		start--
	}
	return result
}

// Splits out the "start" and "ids" properties from encoded revision list
func splitRevisionList(revisions Body) (int, []string) {
	start, ok := base.ToInt64(revisions["start"])
	digests, _ := GetStringArrayProperty(revisions, "ids")
	if ok && len(digests) > 0 && int(start) >= len(digests) {
		return int(start), digests
	} else {
		return 0, nil
	}
}

// Standard CouchDB encoding of a revision list: digests without numeric generation prefixes go in
// the "ids" property, and the first (largest) generation number in the "start" property.
func encodeRevisions(revs []string) Body {
	ids := make([]string, len(revs))
	var start int
	for i, revid := range revs {
		gen, id := ParseRevID(revid)
		ids[i] = id
		if i == 0 {
			start = gen
		} else if gen != start-i {
			base.Warn("encodeRevisions found weird history %v", revs)
		}
	}
	return Body{"start": start, "ids": ids}
}

// Given a revision history encoded by encodeRevisions() and a list of possible ancestor revIDs,
// trim the history to stop at the first ancestor revID. If no ancestors are found, trim to
// length maxUnmatchedLen.
func trimEncodedRevisionsToAncestor(revs Body, ancestors []string, maxUnmatchedLen int) bool {
	start, digests := splitRevisionList(revs)
	if digests == nil {
		return false
	}
	matchIndex := len(digests)
	for _, revID := range ancestors {
		gen, digest := ParseRevID(revID)
		if index := start - gen; index >= 0 && index < matchIndex && digest == digests[index] {
			matchIndex = index
			maxUnmatchedLen = matchIndex + 1
		}
	}
	if maxUnmatchedLen < len(digests) {
		revs["ids"] = digests[0:maxUnmatchedLen]
	}
	return true
}
