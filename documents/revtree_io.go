package documents

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"

	"github.com/couchbase/sync_gateway/base"
)

/*
	V3 REV-TREE SERIALIZED FORMAT:

	A JSON object with the following keys:
	{
		"vers": int
			Version number; currently 3
		"revs3": string
			Concatened revision IDs separated by commas.
		"parents": array of int
			For each revision: the array index of its parent revision, relative to the previous
			parent index. So if the previous parent index was 22, a value of 1 means the parent
			index is 23.
			If a revision has no parent, the parent index is equal to revision itself (which is
			of course not a legal value.) In this case the 'previous parent index' is not updated.
		"flags": string
			This string is interpreted as an array of 6-bit flag bytes of type extRevFlags, where
			each byte is offset by 48 to make it a printable ASCII character.
			The flag bits from low to high are:
				Deleted: 		The RevInfo's Deleted is true.
				HasAttachments: The RevInfo's HasAttachments is true.
				HasBody: 		The RevInfo's Body is not empty nor nil.
				HasNonEmptyBody:The RevInfo's Body is not empty, nil, nor "{}".
				HasBodyKey:		The RevInfo's BodyKey is not empty.
				HasChannels:	The RevInfo's Channels is not empty nor nil.
		"bodies3": array of strings
			The Body values of all revisions whose HasNonEmptyBody flags are set.
			(Other revisions do not appear in this array at all.)
		"bodyKeys": array of strings
			The BodyKey values of all revisions whose HasBodyKey flag is set.
		"channels3": array of array of ints
			The channel sets of all revisions whose HasChannels flag is set.
			Each channel set is encoded as an array of ints. Each int is the index of the channel
			name in the "chanNames" array.
		"chanNames": array of strings
			The unique channel names used in all revisions' Channel sets.
	}

	Some notes on optimizations:

	* Packing the revIDs into one string saves two bytes per rev, but more importantly it means
	  **Go only allocates one string object for the entire RevTree**; the individual RevInfo.ID
	  strings just point inside it.
	* (This isn't related to the encoding format, but:) The implementation allocates all the
	  RevInfo structs as a single array, then populates the map with pointers to the array items.
	  Again, this results in one larger allocation instead of many small ones (and better locality
	  of reference.)
	* The weird encoding of the parents, combined with the order the tree is written (basically
	  breadth-first starting from the leaves), means the parents array consists mostly of "1"s.
	  This was easy to do and cuts the total size by 5% or so.

	Possible further optimizations:

	* It would be possible to omit most of the revIDs' generation numbers, since every rev's
	  generation is its parent's plus one. I tried this, but it required allocating and formatting
	  strings for almost all revisions, which negated a lot of the time and memory savings.
	  So I took that out.
	* The revIDs' digests are hex; base64-encoding them would save 10 bytes each, but the same
	  drawbacks as above would apply.
	* The "parents" and "flags" contain a lot of repeated elements, so run-length encoding is a
	  tempting thing to try. It wouldn't make a huge difference, though; most of the size comes
	  from the revIDs.
	* The channel names are factored out so each appears only once, but that hasn't been done to
	  the channel sets. So the "channels3" property contains redundant sub-arrays like
	  `[[0,1,2],[0,1,2],[1,2,0],[0,1,2],...` This could be optimized too, but in the rev trees
	  I've looked at it wouldn't be a big savings.

	--Jens Alfke, April 2023
*/

// A numeric channel identifier; extRevTree.ChannelNames maps them to strings.
type ChannelID int32

// Defines the JSON format of a serialized RevTree.
type extRevTree struct {
	extRevTreeV2 // Inherits older fields for compatibility; make sure not to duplicate any!

	// Version 3 format:
	Version      int                `json:"vers"`                // JSON format version
	Revs         string             `json:"revs3"`               // The revision IDs concatenated
	Parents      []int              `json:"parents"`             // Relative index of parent (0 if none)
	Flags        string             `json:"flags"`               // Revision flags (really []extRevFlags)
	Bodies       []string           `json:"bodies3,omitempty"`   // Non-default bodies
	BodyKeys     []string           `json:"bodyKeys,omitempty"`  // Body keys
	Channels     [][]ChannelID      `json:"channels3,omitempty"` // Channel sets, as arrays of indices
	ChannelNames NameSet[ChannelID] `json:"chanNames,omitempty"` // Unique channel names
}

// Older format (Version implicitly 2):
type extRevTreeV2 struct {
	Revs2           []string          `json:"revs,omitempty"`       // The revision IDs
	Deleted2        []int             `json:"deleted,omitempty"`    // Indexes of revisions that are deletions
	BodyMap2        map[string]string `json:"bodymap,omitempty"`    // JSON of each revision
	BodyKeyMap2     map[string]string `json:"bodyKeyMap,omitempty"` // Keys of revision bodies stored in external documents
	Channels2       []base.Set        `json:"channels,omitempty"`
	HasAttachments2 []int             `json:"hasAttachments,omitempty"` // Indexes of revisions that has attachments

	// Oldest format:
	Bodies1 []string `json:"bodies,omitempty"` // JSON of each revision (legacy)
}

const kExtRevTreeCurrentVersion = 3

type extRevFlags uint8

const (
	extRevDeleted        extRevFlags = 0x01 // rev.Deleted is true
	extRevHasAttachments extRevFlags = 0x02 // rev.HasAttachments is true
	extRevHasChannels    extRevFlags = 0x04 // rev.Channels exists
	extRevBodyTypeMask   extRevFlags = 0x18 // rev.Body or rev.BodyKey exist -- see below
)

// Values for (flags & extRevBodyTypeMask)
const (
	extRevBodyNone   extRevFlags = 0x00 // No Body or BodyKey
	extRevBodyEmpty  extRevFlags = 0x08 // Body is `{}`
	extRevBodyInline extRevFlags = 0x10 // Body is stored inline, in `bodies3` array
	extRevBodyKey    extRevFlags = 0x18 // BodyKey is in `bodyKeys` array; no Body
)

var kEmptyBody = []byte{'{', '}'}

const kAvgRevIDLen = 36  // = 2-digit gen + '-' + 32-byte digest + ','
const kFlagsToASCII = 48 // Offset when converting extRevFlags to ASCII

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
		if len(rev.BodyKey) > 0 {
			flag |= extRevBodyKey
		} else if len(rev.Body) > 0 {
			if len(rev.Body) > 2 { // i.e. body is not "{}"
				flag |= extRevBodyInline
			} else {
				flag |= extRevBodyEmpty
			}
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
	flags := make([]extRevFlags, n)                            // Array of flags

	addRev := func(rev *RevInfo) int {
		// Adds a RevInfo to `ext`
		revIndex++
		rev.scratch = uint32(revIndex)
		infos[revIndex] = rev
		if revIndex > 0 {
			revIDs.WriteByte(',')
		}
		revIDs.WriteString(rev.ID)

		revFlags := mkFlags(rev)
		switch revFlags & extRevBodyTypeMask {
		case extRevBodyKey:
			ext.BodyKeys = append(ext.BodyKeys, string(rev.BodyKey))
		case extRevBodyInline:
			ext.Bodies = append(ext.Bodies, string(rev.Body))
		}
		if (revFlags & extRevHasChannels) != 0 {
			ext.Channels = append(ext.Channels, encodeChannels(rev.Channels))
		}
		flags[revIndex] = revFlags + kFlagsToASCII
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
	lastParentIndex := 0
	for i := 0; i < n; i++ {
		if parent := infos[i].Parent; parent != nil {
			parentIndex := int(parent.scratch)
			if parentIndex >= n {
				parentIndex = addRev(parent)
			}
			ext.Parents[i] = parentIndex - lastParentIndex
			lastParentIndex = parentIndex
		} else {
			ext.Parents[i] = i - lastParentIndex
		}
	}
	ext.Revs = revIDs.String()
	ext.Flags = string(flags)

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
	var ext extRevTree
	if err := base.JSONUnmarshal(tree.jsonForm, &ext); err != nil {
		return err
	} else if ext.Version < 3 {
		return tree.unmarshalv2(&ext)
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

	n := len(ext.Parents) // Number of revs
	var revIDs []string   // Array of revIDs
	if len(ext.Revs) > 0 {
		revIDs = strings.Split(ext.Revs, ",")
	}
	if len(revIDs) != n || len(ext.Flags) != n {
		return fmt.Errorf("encoded RevTree has inconsistent number of revs: revIDs=%d, parents=%d, flags=%d ... %s", len(revIDs), n, len(ext.Flags), tree.jsonForm)
	}

	revArray := make([]RevInfo, n) // For efficiency, allocate all the RevInfos in one array
	tree.revs = make(RevMap, n)
	for i := 0; i < n; i++ {
		rev := &revArray[i]
		rev.ID = revIDs[i]
		rev.Leaf = true
		tree.revs[rev.ID] = rev
	}

	bodyIndex := 0     // Next item in ext.Bodies
	bodyKeyIndex := 0  // Next item in ext.BodyKeys
	channelsIndex := 0 // Next item in ext.Channels
	lastParent := 0

	for i, flagChar := range ext.Flags {
		rev := &revArray[i]
		if parentIndex := lastParent + ext.Parents[i]; parentIndex != i {
			if parentIndex < 0 || parentIndex >= n {
				return fmt.Errorf("encoded RevTree has invalid parent index")
			}
			rev.Parent = &revArray[parentIndex]
			rev.Parent.Leaf = false
			lastParent = parentIndex
		}

		revFlags := extRevFlags(flagChar - kFlagsToASCII)
		rev.Deleted = (revFlags & extRevDeleted) != 0
		rev.HasAttachments = (revFlags & extRevHasAttachments) != 0

		switch revFlags & extRevBodyTypeMask {
		case extRevBodyKey:
			if bodyKeyIndex >= len(ext.BodyKeys) {
				return fmt.Errorf("encoded RevTree has too few bodyKeys")
			}
			rev.BodyKey = ext.BodyKeys[bodyKeyIndex]
			bodyKeyIndex++
		case extRevBodyInline:
			if bodyIndex >= len(ext.Bodies) {
				return fmt.Errorf("encoded RevTree has too few bodies")
			}
			rev.Body = []byte(ext.Bodies[bodyIndex])
			bodyIndex++
		case extRevBodyEmpty:
			rev.Body = kEmptyBody
		}

		if (revFlags & extRevHasChannels) != 0 {
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

func (tree *RevTree) unmarshalv2(rep *extRevTree) (err error) {
	// validate revTreeList revs, parents and channels lists are of equal length
	n := len(rep.Revs2)
	if !(n == len(rep.Parents) && n == len(rep.Channels2)) {
		return errors.New("encoded RevTree (v2) has inconsistent revs/parents/channels counts")
	}

	revArray := make([]RevInfo, n) // For efficiency, allocate all the RevInfos in one array
	tree.revs = make(RevMap, n)
	for i := 0; i < n; i++ {
		revArray[i].Leaf = true
	}

	for i, revid := range rep.Revs2 {
		info := &revArray[i]
		info.ID = revid
		stringIndex := strconv.FormatInt(int64(i), 10)
		if rep.BodyMap2 != nil {
			if body := rep.BodyMap2[stringIndex]; body != "" {
				info.Body = []byte(body)
			}
		} else if rep.Bodies1 != nil && len(rep.Bodies1[i]) > 0 {
			info.Body = []byte(rep.Bodies1[i])
		}
		if rep.BodyKeyMap2 != nil {
			bodyKey, ok := rep.BodyKeyMap2[stringIndex]
			if ok {
				info.BodyKey = bodyKey
			}
		}
		if rep.Channels2 != nil {
			info.Channels = rep.Channels2[i]
		}
		parentIndex := rep.Parents[i]
		if parentIndex >= 0 {
			info.Parent = &revArray[parentIndex]
			info.Parent.Leaf = false
		}
		tree.revs[revid] = info
	}
	if rep.Deleted2 != nil {
		for _, i := range rep.Deleted2 {
			info := tree.revs[rep.Revs2[i]]
			info.Deleted = true
		}
	}
	if rep.HasAttachments2 != nil {
		for _, i := range rep.HasAttachments2 {
			info := tree.revs[rep.Revs2[i]]
			info.HasAttachments = true
		}

	}
	return
}
