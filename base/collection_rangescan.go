/*
Copyright 2025-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"fmt"

	"github.com/couchbase/gocb/v2"
	sgbucket "github.com/couchbase/sg-bucket"
)

var _ sgbucket.RangeScanStore = &Collection{}

func (c *Collection) Scan(scanType sgbucket.ScanType, opts sgbucket.ScanOptions) (sgbucket.ScanResultIterator, error) {
	c.Bucket.waitForAvailKvOp()
	defer c.Bucket.releaseKvOp()

	gocbScanType, err := toGocbScanType(scanType)
	if err != nil {
		return nil, err
	}

	scanOpts := &gocb.ScanOptions{
		IDsOnly:    opts.IDsOnly,
		Transcoder: gocb.NewRawJSONTranscoder(),
	}

	result, err := c.Collection.Scan(gocbScanType, scanOpts)
	if err != nil {
		return nil, err
	}

	return &gocbScanResultIterator{result: result, idsOnly: opts.IDsOnly}, nil
}

func toGocbScanType(scanType sgbucket.ScanType) (gocb.ScanType, error) {
	switch st := scanType.(type) {
	case sgbucket.RangeScan:
		rs := gocb.RangeScan{}
		if st.From != nil {
			rs.From = &gocb.ScanTerm{Term: st.From.Term, Exclusive: st.From.Exclusive}
		}
		if st.To != nil {
			rs.To = &gocb.ScanTerm{Term: st.To.Term, Exclusive: st.To.Exclusive}
		}
		return rs, nil
	default:
		return nil, fmt.Errorf("unsupported scan type: %T", scanType)
	}
}

type gocbScanResultIterator struct {
	result  *gocb.ScanResult
	idsOnly bool
}

func (it *gocbScanResultIterator) Next() *sgbucket.ScanResultItem {
	item := it.result.Next()
	if item == nil {
		return nil
	}
	result := &sgbucket.ScanResultItem{
		ID:     item.ID(),
		Cas:    uint64(item.Cas()),
		IDOnly: item.IDOnly(),
	}
	if !item.IDOnly() {
		var body []byte
		if err := item.Content(&body); err == nil {
			result.Body = body
		}
	}
	return result
}

func (it *gocbScanResultIterator) Close() error {
	return it.result.Close()
}
