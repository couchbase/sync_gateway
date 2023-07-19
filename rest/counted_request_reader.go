// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"io"
)

type CountedRequestReader struct {
	numBytes int64
	reader   io.Reader
}

// NewReaderCounter returns a new CountedRequestReader storing the io.Reader and bytes read off the reader
func NewReaderCounter(reader io.Reader) *CountedRequestReader {
	return &CountedRequestReader{
		reader: reader,
	}
}

// Read overrides the Read method from io package that will add the number of bytes read to the CountedRequestReader struct for
// retrieval for the stat on the database
func (c *CountedRequestReader) Read(buf []byte) (int, error) {
	numBytesRead, err := c.reader.Read(buf)

	c.numBytes += int64(numBytesRead)
	return numBytesRead, err
}

// LoadCount will load the number of bytes read for the request from the CountedRequestReader struct
func (c *CountedRequestReader) LoadCount() int64 {
	return c.numBytes
}

// Close to satisfy the interface
func (c *CountedRequestReader) Close() error { return nil }
