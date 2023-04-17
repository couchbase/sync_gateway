package documents

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/couchbase/sync_gateway/base"
)

// A numeric channel identifier; extRevTree.ChannelNames maps them to strings.
type ChannelID int32

// used for JSON marshaling of RevTree
type extRevTree struct {
	Version      int                `json:"vers"`                // JSON format version
	Revs         string             `json:"revs"`                // The revision IDs concatenated
	Parents      []int              `json:"parents"`             // Relative index of parent (0 if none)
	Flags        []extRevFlags      `json:"flags"`               // Flags for each revision
	Bodies       [][]byte           `json:"bodies,omitempty"`    // Non-default bodies
	BodyKeys     []string           `json:"bodyKeys,omitempty"`  // Body keys
	Channels     [][]ChannelID      `json:"channels,omitempty"`  // Channel sets, as arrays of indices
	ChannelNames NameSet[ChannelID] `json:"chanNames,omitempty"` // Unique channel names
}

const kExtRevTreeCurrentVersion = 3

type extRevFlags uint8

const (
	extRevDeleted         extRevFlags = 1  // rev.Deleted is true
	extRevHasAttachments  extRevFlags = 2  // rev.HasAttachments is true
	extRevHasBody         extRevFlags = 4  // rev.Body exists
	extRevHasNonEmptyBody extRevFlags = 8  // rev.Body exists and is not "{}"
	extRevHasBodyKey      extRevFlags = 16 // rev.BodyKey exists
	extRevHasChannels     extRevFlags = 32 // rev.Channels exists
)

var kEmptyBody = []byte{'{', '}'}

const kAvgRevIDLen = 36 // 2-digit gen, '-', 32-byte digest, ','

func (tree RevTree) MarshalJSON() ([]byte, error) {
	if tree.jsonForm != nil {
		// If I haven't been unmarshaled yet, just return the original JSON
		return tree.jsonForm, nil
	}

	// ---- variables and subroutines

	n := tree.RevCount()
	ext := extRevTree{
		Version: kExtRevTreeCurrentVersion,
		Parents: make([]int, n),
		Flags:   make([]extRevFlags, n),
	}

	mkFlags := func(rev *RevInfo) extRevFlags {
		// Computes the extRevFlags for a RevInfo.
		var flag extRevFlags
		if rev.Deleted {
			flag |= extRevDeleted
		}
		if rev.HasAttachments {
			flag |= extRevHasAttachments
		}
		if len(rev.Body) > 0 {
			flag |= extRevHasBody
			if len(rev.Body) > 2 { // i.e. body is not "{}"
				flag |= extRevHasNonEmptyBody
			}
		}
		if len(rev.BodyKey) > 0 {
			flag |= extRevHasBodyKey
		}
		if len(rev.Channels) > 0 {
			flag |= extRevHasChannels
		}
		return flag
	}

	encodeChannels := func(channels base.Set) []ChannelID {
		// Converts RevInfo.Channels set into an array of indices in extRevTree.ChannelNames[]
		if len(channels) == 0 {
			return nil
		}
		result := make([]ChannelID, 0, len(channels))
		for ch := range channels {
			result = append(result, ext.ChannelNames.Add(ch))
		}
		return result
	}

	revIDs := bytes.NewBuffer(make([]byte, 0, n*kAvgRevIDLen)) // Concatenated revIDs
	revIndex := -1                                             // Current rev index
	infos := make([]*RevInfo, n)                               // Maps index to RevInfo

	addRev := func(rev *RevInfo) int {
		// Adds a RevInfo to `ext`
		revIndex++
		rev.scratch = uint32(revIndex)
		infos[revIndex] = rev
		if revIndex > 0 {
			revIDs.WriteByte(',')
		}
		revIDs.WriteString(rev.ID)
		flags := mkFlags(rev)
		ext.Flags[revIndex] = flags + 32 // Convert flags to printable ASCII char
		if (flags & extRevHasNonEmptyBody) != 0 {
			ext.Bodies = append(ext.Bodies, rev.Body)
		}
		if (flags & extRevHasBodyKey) != 0 {
			ext.BodyKeys = append(ext.BodyKeys, string(rev.BodyKey))
		}
		if (flags & extRevHasChannels) != 0 {
			ext.Channels = append(ext.Channels, encodeChannels(rev.Channels))
		}
		return revIndex
	}

	// ---- Now the code:

	// First, add the leaf revs; addRev will initialize their `scratch` to their array index.
	// Other revs' `scratch` will be initialized to an illegal value to show they're not added yet.
	for _, rev := range tree.revs {
		if rev.Leaf {
			addRev(rev)
		} else {
			rev.scratch = math.MaxUint32
		}
	}

	// Now walk through the Revs array and add each revision's parent to it:
	for i := 0; i < n; i++ {
		if parent := infos[i].Parent; parent != nil {
			parentIndex := int(parent.scratch)
			if parentIndex >= n {
				parentIndex = addRev(parent)
			}
			ext.Parents[i] = parentIndex - i
		}
	}
	ext.Revs = revIDs.String()

	// Finally return the JSON encoding of the extRevTree:
	return base.JSONMarshal(ext)
}

func (tree *RevTree) UnmarshalJSON(inputjson []byte) (err error) {
	// Be lazy: just save the JSON until the first time I'm accessed (see lazyLoadJSON)
	tree.jsonForm = inputjson
	tree.revs = nil
	tree.parseErr = nil
	return nil
}

// Actually unmarshals the JSON saved in `jsonForm`
func (tree *RevTree) lazyLoadJSON() (err error) {
	// Quick test for old format, which doesn't have a "vers" property:
	if !bytes.Contains(tree.jsonForm, []byte(`"vers":`)) {
		return tree.oldUnmarshalJSON(tree.jsonForm)
	}

	var ext extRevTree
	if err := base.JSONUnmarshal(tree.jsonForm, &ext); err != nil {
		return err
	}
	if ext.Version == 0 {
		return tree.oldUnmarshalJSON(tree.jsonForm)
	} else if ext.Version != kExtRevTreeCurrentVersion {
		return fmt.Errorf("encoded RevTree has incompatible version number")
	}

	decodeChannels := func(indexes []ChannelID) base.Set {
		// converts an array of channel indices back into a Set
		if len(indexes) == 0 {
			return nil
		}
		channels := make(base.Set, len(indexes))
		for _, i := range indexes {
			if ch, found := ext.ChannelNames.GetString(i); found {
				channels.Add(ch)
			}
		}
		return channels
	}

	n := len(ext.Flags)                    // Number of revs
	revArray := make([]RevInfo, n)         // For efficiency, allocate all the RevInfos in one array
	revIDs := strings.Split(ext.Revs, ",") // Array of revIDs
	if len(revIDs) != n {
		return fmt.Errorf("encoded RevTree has inconsistent number of revs")
	}

	tree.revs = make(RevMap, n)
	for i := 0; i < n; i++ {
		rev := &revArray[i]
		rev.ID = revIDs[i]
		tree.revs[rev.ID] = rev
		rev.Leaf = true
	}

	bodyIndex := 0     // Next item in ext.Bodies
	bodyKeyIndex := 0  // Next item in ext.BodyKeys
	channelsIndex := 0 // Next item in ext.Channels

	for i, flags := range ext.Flags {
		flags -= 32 // Convert printable ASCII char back to flags
		rev := &revArray[i]
		rev.Deleted = (flags & extRevDeleted) != 0
		rev.HasAttachments = (flags & extRevHasAttachments) != 0

		if parentOffset := ext.Parents[i]; parentOffset != 0 {
			if parent := i + parentOffset; parent >= 0 && parent < n {
				rev.Parent = &revArray[i+parentOffset]
				rev.Parent.Leaf = false
			} else {
				return fmt.Errorf("encoded RevTree has invalid parent offset")
			}
		}
		if (flags & extRevHasBody) != 0 {
			if (flags & extRevHasNonEmptyBody) != 0 {
				if bodyIndex >= len(ext.Bodies) {
					return fmt.Errorf("encoded RevTree has too few bodies")
				}
				rev.Body = ext.Bodies[bodyIndex]
				bodyIndex++
			} else {
				rev.Body = kEmptyBody
			}
		}
		if (flags & extRevHasBodyKey) != 0 {
			if bodyKeyIndex >= len(ext.BodyKeys) {
				return fmt.Errorf("encoded RevTree has too few bodyKeys")
			}
			rev.BodyKey = ext.BodyKeys[bodyKeyIndex]
			bodyKeyIndex++
		}
		if (flags & extRevHasChannels) != 0 {
			if channelsIndex >= len(ext.Channels) {
				return fmt.Errorf("encoded RevTree has too few channels")
			}
			rev.Channels = decodeChannels(ext.Channels[channelsIndex])
			channelsIndex++
		}
	}
	return nil
}

//////// UNMARSHALING OLDER FORMAT:

// The old form in which a RevTree was stored in JSON.
type revTreeList struct {
	Revs           []string          `json:"revs"`                 // The revision IDs
	Parents        []int             `json:"parents"`              // Index of parent of each revision (-1 if root)
	Deleted        []int             `json:"deleted,omitempty"`    // Indexes of revisions that are deletions
	Bodies_Old     []string          `json:"bodies,omitempty"`     // JSON of each revision (legacy)
	BodyMap        map[string]string `json:"bodymap,omitempty"`    // JSON of each revision
	BodyKeyMap     map[string]string `json:"bodyKeyMap,omitempty"` // Keys of revision bodies stored in external documents
	Channels       []base.Set        `json:"channels"`
	HasAttachments []int             `json:"hasAttachments,omitempty"` // Indexes of revisions that has attachments
}

func (tree *RevTree) oldUnmarshalJSON(inputjson []byte) (err error) {
	var rep revTreeList
	err = base.JSONUnmarshal(inputjson, &rep)
	if err != nil {
		panic(fmt.Sprintf("Couldn't unmarshal to revTreeList: %s\nerror: %s", inputjson, err)) //TEMP
		return
	}

	// validate revTreeList revs, parents and channels lists are of equal length
	n := len(rep.Revs)
	if !(n == len(rep.Parents) && n == len(rep.Channels)) {
		return errors.New("revtreelist data is invalid, revs/parents/channels counts are inconsistent")
	}

	revArray := make([]RevInfo, n) // For efficiency, allocate all the RevInfos in one array
	tree.revs = make(RevMap, n)
	for i := 0; i < n; i++ {
		revArray[i].Leaf = true
	}

	for i, revid := range rep.Revs {
		info := &revArray[i]
		info.ID = revid
		stringIndex := strconv.FormatInt(int64(i), 10)
		if rep.BodyMap != nil {
			if body := rep.BodyMap[stringIndex]; body != "" {
				info.Body = []byte(body)
			}
		} else if rep.Bodies_Old != nil && len(rep.Bodies_Old[i]) > 0 {
			info.Body = []byte(rep.Bodies_Old[i])
		}
		if rep.BodyKeyMap != nil {
			bodyKey, ok := rep.BodyKeyMap[stringIndex]
			if ok {
				info.BodyKey = bodyKey
			}
		}
		if rep.Channels != nil {
			info.Channels = rep.Channels[i]
		}
		parentIndex := rep.Parents[i]
		if parentIndex >= 0 {
			info.Parent = &revArray[parentIndex]
			info.Parent.Leaf = false
		}
		tree.revs[revid] = info
	}
	if rep.Deleted != nil {
		for _, i := range rep.Deleted {
			info := tree.revs[rep.Revs[i]]
			info.Deleted = true // because tree.revs[rep.Revs[i]].Deleted=true is a compile error
			tree.revs[rep.Revs[i]] = info
		}
	}
	if rep.HasAttachments != nil {
		for _, i := range rep.HasAttachments {
			info := tree.revs[rep.Revs[i]]
			info.HasAttachments = true
			tree.revs[rep.Revs[i]] = info
		}

	}
	return
}
