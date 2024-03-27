//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package mqtt

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStateTemplate(t *testing.T) {
	template := Body{"temp": "${message.payload.temperature}", "time": "${date}", "foo": "bar"}
	topic := TopicMatch{Name: "temp"}

	payload := Body{"temperature": 98.5}
	ts, _ := time.Parse(time.RFC3339, "2024-04-11T12:41:31.016-07:00")
	result, err := applyStateTemplate(template, payload, ts, topic)

	assert.NoError(t, err)
	assert.Equal(t, Body{"temp": 98.5, "time": "2024-04-11T12:41:31.016-07:00", "foo": "bar"}, result)
}

func TestTimeSeriesTemplate(t *testing.T) {
	cfg := TimeSeriesConfig{
		TimeProperty:   "${message.payload.when}",
		ValuesTemplate: []any{"${message.payload.temperature}"},
	}

	payload := Body{"temperature": 98.5, "when": "2024-04-11T12:42:19.643-07:00"}
	ts, _ := time.Parse(time.RFC3339, "2024-04-11T12:41:31.016-07:00")

	result, err := applyTimeSeriesTemplate(&cfg, payload, ts, false)
	assert.NoError(t, err)
	assert.EqualValues(t, []any{int64(1712864539), 98.5}, result)
}
