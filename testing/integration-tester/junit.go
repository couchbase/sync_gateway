// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package main

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"strings"
)

// prefixTestcaseClassnames reads a JUnit XML file, prepends prefix to every
// testcase/@classname attribute value, and writes the result to output.
// It is equivalent to: xmlstarlet ed -u '//testcase/@classname' -x 'concat("<prefix>", .)'
func prefixTestcaseClassnames(input, output, prefix string) error {
	logger.Debugf("prefixTestcaseClassnames %q -> %q (prefix=%q)", input, output, prefix)
	data, err := os.ReadFile(input) //nolint:gosec
	if err != nil {
		return fmt.Errorf("read %q: %w", input, err)
	}
	dec := xml.NewDecoder(bytes.NewReader(data))
	var buf bytes.Buffer
	enc := xml.NewEncoder(&buf)
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("decode %q: %w", input, err)
		}
		if se, ok := tok.(xml.StartElement); ok && se.Name.Local == "testcase" {
			for i, attr := range se.Attr {
				if attr.Name.Local == "classname" {
					se.Attr[i].Value = prefix + attr.Value
					break
				}
			}
			tok = se
		}
		if err := enc.EncodeToken(tok); err != nil {
			return fmt.Errorf("encode token: %w", err)
		}
	}
	if err := enc.Flush(); err != nil {
		return fmt.Errorf("flush encoder: %w", err)
	}
	if err := os.WriteFile(output, buf.Bytes(), 0o600); err != nil { //nolint:gosec
		return fmt.Errorf("write %q: %w", output, err)
	}
	return nil
}

// mergeJUnitXML merges the <testsuite> elements from each result's XML file into
// a single output file wrapped in a <testsuites> root element.
func mergeJUnitXML(results []result, output string) {
	logger.Debugf("mergeJUnitXML: merging %d result(s) into %q", len(results), output)
	var b strings.Builder
	b.WriteString(xml.Header + "<testsuites>\n")
	for _, r := range results {
		if r.xmlFile == "" {
			continue
		}
		data, err := os.ReadFile(r.xmlFile) //nolint:gosec
		if err != nil {
			nonFatal.add(fmt.Errorf("read xml %q: %w", r.xmlFile, err))
			continue
		}
		suites, err := extractTestSuites(data)
		if err != nil {
			nonFatal.add(fmt.Errorf("parse xml %q: %w", r.xmlFile, err))
			continue
		}
		for _, suite := range suites {
			b.WriteString(string(suite))
			b.WriteByte('\n')
		}
	}
	b.WriteString("</testsuites>\n")
	if err := os.WriteFile(output, []byte(b.String()), 0o600); err != nil { //nolint:gosec
		logger.Fatalf("write merged xml %q: %v", output, err)
	}
}

// extractTestSuites returns the raw bytes of each <testsuite>...</testsuite> element
// found in data, regardless of nesting depth.
//
// Token() is used (not RawToken()) so that the decoder's element stack stays in sync;
// Skip() calls Token() internally and returns "unexpected end element" if the stack
// is empty when it reads </testsuite>.
func extractTestSuites(data []byte) ([][]byte, error) {
	dec := xml.NewDecoder(bytes.NewReader(data))
	var suites [][]byte
	for {
		tokenStart := dec.InputOffset()
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		se, ok := tok.(xml.StartElement)
		if !ok || se.Name.Local != "testsuite" {
			continue
		}
		if err := dec.Skip(); err != nil {
			return nil, err
		}
		suites = append(suites, bytes.TrimSpace(data[tokenStart:dec.InputOffset()]))
	}
	return suites, nil
}
