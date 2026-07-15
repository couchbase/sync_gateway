// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package jsbench

import (
	"fmt"
	"strings"
)

const tinySource = `function(doc) {
	channel(doc.channel);
}`

var tinyDoc = map[string]interface{}{
	"_id":     "doc1",
	"channel": "general",
}

const smallSource = `function(doc, oldDoc) {
	if (doc.type == "invoice") {
		channel("invoices");
		access(doc.owner, "invoices");
	} else if (doc.type == "receipt") {
		channel("receipts");
	} else {
		channel("misc");
	}
	if (doc.public) {
		channel("!");
	}
}`

var smallDoc = map[string]interface{}{
	"_id":    "doc2",
	"type":   "invoice",
	"owner":  "alice",
	"public": false,
}

const mediumSource = `function(doc, oldDoc) {
	var chans = [];
	for (var i = 0; i < doc.tags.length; i++) {
		chans.push("tag:" + doc.tags[i]);
	}
	channel(chans);

	var roles = [];
	for (var j = 0; j < doc.members.length; j++) {
		var m = doc.members[j];
		if (m.active) {
			access(m.name, "team:" + doc.teamId);
			roles.push(m.name);
		}
	}
	role(roles, "role:member");

	var summary = JSON.stringify({id: doc._id, tags: chans.length, members: roles.length});
	console.log(summary);
}`

var mediumDoc = map[string]interface{}{
	"_id":    "doc3",
	"teamId": "team42",
	"tags":   []interface{}{"red", "blue", "green", "yellow", "purple"},
	"members": []interface{}{
		map[string]interface{}{"name": "alice", "active": true},
		map[string]interface{}{"name": "bob", "active": false},
		map[string]interface{}{"name": "carol", "active": true},
		map[string]interface{}{"name": "dave", "active": true},
	},
}

// buildLargeSource generates a large, repetitive sync function (~n "document type" branches
// plus a nested-loop aggregation section) to approximate a big, generated/templated real-world
// sync function.
func buildLargeSource(n int) string {
	var b strings.Builder
	b.WriteString("function(doc, oldDoc, meta) {\n")
	b.WriteString("\tvar chans = [];\n")
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "\tif (doc.type == \"type%d\") {\n", i)
		fmt.Fprintf(&b, "\t\tchans.push(\"chan%d\");\n", i)
		fmt.Fprintf(&b, "\t\taccess(doc.owner, \"chan%d\");\n", i)
		fmt.Fprintf(&b, "\t\tif (doc.priority > %d) { role(doc.owner, \"role:escalated\"); }\n", i%5)
		b.WriteString("\t}\n")
	}
	b.WriteString("\tchannel(chans);\n")
	b.WriteString("\tvar total = 0;\n")
	b.WriteString("\tfor (var i = 0; i < doc.history.length; i++) {\n")
	b.WriteString("\t\tvar entry = doc.history[i];\n")
	b.WriteString("\t\tfor (var j = 0; j < entry.events.length; j++) {\n")
	b.WriteString("\t\t\ttotal += entry.events[j].amount;\n")
	b.WriteString("\t\t}\n")
	b.WriteString("\t}\n")
	b.WriteString("\tif (total > 1000) { channel(\"high-value\"); }\n")
	b.WriteString("\tvar summary = JSON.stringify({id: doc._id, total: total, chans: chans.length});\n")
	b.WriteString("\tconsole.log(summary);\n")
	b.WriteString("}")
	return b.String()
}

var largeSource = buildLargeSource(60)

func buildLargeDoc() map[string]interface{} {
	history := make([]interface{}, 20)
	for i := range history {
		events := make([]interface{}, 10)
		for j := range events {
			events[j] = map[string]interface{}{"amount": float64(i*10 + j)}
		}
		history[i] = map[string]interface{}{"events": events}
	}
	return map[string]interface{}{
		"_id":      "doc4",
		"type":     "type42",
		"owner":    "erin",
		"priority": 3,
		"history":  history,
	}
}

var largeDoc = buildLargeDoc()
