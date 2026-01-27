// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package db

import (
	"fmt"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/require"
)

func TestBlipSequenceProperty(t *testing.T) {
	testCases := []struct {
		name                 string
		seq                  SequenceID
		expectedString       string
		expectedISGRV2String string
	}{
		{
			name:                 "Simple SequenceID",
			seq:                  SequenceID{Seq: 1},
			expectedString:       `1`,
			expectedISGRV2String: `1`,
		},
		{
			name:                 "compound sequenceID",
			seq:                  SequenceID{LowSeq: 5, TriggeredBy: 10, Seq: 15},
			expectedString:       `"5:10:15"`,
			expectedISGRV2String: `5:10:15`,
		},
		{
			name:                 "TriggeredBy only",
			seq:                  SequenceID{TriggeredBy: 20, Seq: 25},
			expectedString:       `"20:25"`,
			expectedISGRV2String: `20:25`,
		},
		{
			name:                 "LowSeq and Seq",
			seq:                  SequenceID{LowSeq: 30, Seq: 35},
			expectedString:       `"30::35"`,
			expectedISGRV2String: `30::35`,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// test norev sequence property
			norevMessage := NewNoRevMessage()
			require.NoError(t, norevMessage.SetSequence(tc.seq))
			require.Equal(t, tc.expectedString, norevMessage.Properties[NorevMessageSequence])
			// this string is only used by ISGR when CB_Mobile < 3
			norevMessage.SetSeq(tc.seq)
			require.Equal(t, tc.expectedISGRV2String, norevMessage.Properties[NorevMessageSeq])

			// test rev sequence property
			properties, err := blipRevMessageProperties(nil, false, tc.seq, "", nil)
			require.NoError(t, err)
			require.Equal(t, tc.expectedString, properties[RevMessageSequence])
			changeEntry := &ChangeEntry{
				Seq: tc.seq,
				ID:  "docID",
			}
			// changes message format
			ctx := base.TestCtx(t)
			bh := &blipHandler{
				loggingCtx:      ctx,
				BlipSyncContext: &BlipSyncContext{},
			}
			changeRow := bh.buildChangesRow(changeEntry, ChangeByVersionType{"rev": "1-abc"})
			rowString, err := base.JSONMarshal(changeRow)
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf(`[%s,"docID","1-abc"]`, tc.expectedString), string(rowString))
		})
	}

}
