//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package mqtt

import (
	"fmt"
	"net/url"
	"sync"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
)

// Per-database MQTT configuration. (Implements db.MQTTConfig)
type PerDBConfig struct {
	Enabled *bool           `json:"enabled,omitempty"`
	Clients []*ClientConfig `json:"clients,omitempty"`
	Broker  *BrokerConfig   `json:"broker,omitempty"`
}

//======== PER-DB MQTT CLIENT CONFIG

// Configuration for MQTT client
type ClientConfig struct {
	Enabled *bool              `json:"enabled,omitempty"` // `false` to disable client
	Broker  RemoteBrokerConfig `json:"broker"`            // How to connect to the broker
	Ingest  IngestMap          `json:"ingest"`            // Topics to subscribe to (key is topic filter)
}

// Location & credentials for connecting to a broker as a client
type RemoteBrokerConfig struct {
	URL      string  `json:"url"`                 // Broker URL to connect to (mqtt:, mqtts:, ws:, wss:)
	User     *string `json:"user"`                // Username to log in with
	Password *string `json:"password"`            // Password to log in with
	ClientID string  `json:"client_id,omitempty"` // Client ID; must be unique per SG node
}

//TODO: Support client-cert auth; any others?

//======== PER-DB MQTT BROKER/SERVER CONFIG

// Configuration for a DB being served via SG's MQTT broker.
type BrokerConfig struct {
	Enabled       *bool                    `json:"enabled,omitempty"` // False to disable broker
	Allow         []*TopicConfig           `json:"allow,omitempty"`   // Maps topic filter -> ACLs
	Ingest        IngestMap                `json:"ingest,omitempty"`  // Topics to save as docs
	mutex         sync.Mutex               `json:"-"`                 // Lock to access fields below
	allowFilters  *TopicMap[*TopicConfig]  `json:"-"`                 // Compiled from 'Allow'
	ingestFilters *TopicMap[*IngestConfig] `json:"-"`                 // Compiled from 'Ingest'
}

// ACLs for user access to served topics
type TopicConfig struct {
	Topic     string    `json:"topic"`               // Topic name or filter
	Publish   ACLConfig `json:"publish"`             // ACLs for publishing to this topic
	Subscribe ACLConfig `json:"subscribe,omitempty"` // ACLs for subscribing to this topic
}

type ACLConfig struct {
	Channels []string `json:"channels,omitempty"` // Users with access to these channels are allowed
	Roles    []string `json:"roles,omitempty"`    // Users with these roles are allowed
	Users    []string `json:"users,omitempty"`    // Users with these names/patterns are allowed
}

//======== SHARED "INGEST" CONFIG

const (
	ModelState           = "state"             // Each message replaces the document
	ModelTimeSeries      = "time_series"       // Message is added to `ts_data` key
	ModelSpaceTimeSeries = "space_time_series" // Message is added to `ts_data` key w/coords

	EncodingString = "string" // Payload will be saved as a string (if it's valid UTF-8)
	EncodingBase64 = "base64" // Payload will be a base64-encoded string
	EncodingJSON   = "JSON"   // Payload interpreted as JSON
)

type Body = map[string]any

// Describes how to save incoming messages from a single MQTT topic.
type IngestConfig struct {
	DocID           string                 `json:"doc_id,omitempty"`           // docID to write topic messages to
	Scope           string                 `json:"scope,omitempty"`            // Scope to save to
	Collection      string                 `json:"collection,omitempty"`       // Collection to save to
	Encoding        *string                `json:"payload_encoding,omitempty"` // How to parse payload (default "string")
	Model           *string                `json:"model,omitempty"`            // Save mode: "state" (default) or "time_series"
	StateTemplate   Body                   `json:"state,omitempty"`            // Document properties template
	TimeSeries      *TimeSeriesConfig      `json:"time_series,omitempty"`
	SpaceTimeSeries *SpaceTimeSeriesConfig `json:"space_time_series,omitempty"`
	QoS             *int                   `json:"qos,omitempty"`      //  QoS of subscription, client-side only (default: 2)
	Channels        []string               `json:"channels,omitempty"` // Channel access of doc
}

type IngestMap map[string]*IngestConfig

const kDefaultRotationInterval = 24 * time.Hour
const kDefaultRotationMaxSize = 10_000_000

type TimeSeriesConfig struct {
	TimeProperty    string `json:"time,omitempty"`                    // Pattern expanding to timestamp
	TimeFormat      string `json:"time_format,omitempty"`             // How to parse timestamp
	ValuesTemplate  []any  `json:"values"`                            // Values to put in TS entry
	OtherProperties Body   `json:"other_properties,omitempty"`        // Other properties to add to doc
	Rotation        string `json:"rotation,omitempty"`                // Time interval to rotate docs
	RotationMaxSize int    `json:"rotation_max_size_bytes,omitempty"` // Size in bytes to rotate docs at

	rotationInterval time.Duration // Parsed from Rotation
}

type SpaceTimeSeriesConfig struct {
	TimeSeriesConfig
	Latitude  string `json:"latitude"`  // Pattern expanding to latitude
	Longitude string `json:"longitude"` // Pattern expanding to longitude
}

//======== VALIDATION:

func (config *PerDBConfig) IsEnabled() bool {
	return config != nil && (config.Enabled == nil || *config.Enabled)
}

func (config *ClientConfig) IsEnabled() bool {
	return config != nil && (config.Enabled == nil || *config.Enabled)
}

func (config *BrokerConfig) IsEnabled() bool {
	return config != nil && (config.Enabled == nil || *config.Enabled)
}

func (bc *BrokerConfig) Validate() error {
	var authFilters TopicMap[*TopicConfig]
	for _, tc := range bc.Allow {
		if err := authFilters.AddFilter(tc.Topic, tc); err != nil {
			return err
		}
	}
	return validateSubscriptions(bc.Ingest)
}

func (config *ClientConfig) Validate() error {
	if url, _ := url.Parse(config.Broker.URL); url == nil {
		return fmt.Errorf("invalid broker URL `%s`", config.Broker.URL)
	}
	return validateSubscriptions(config.Ingest)
}

func (config *TimeSeriesConfig) Validate() error {
	if config != nil {
		// pass allowMissingProperties=true to suppress errors due to the empty payload
		if _, err := applyTimeSeriesTemplate(config, []any{}, time.Unix(0, 0), true); err != nil {
			return err
		} else if err = config.validateRotation(); err != nil {
			return err
		}
	}
	return nil
}

func (config *SpaceTimeSeriesConfig) Validate() error {
	if config != nil {
		// pass allowMissingProperties=true to suppress errors due to the empty payload
		if _, err := applySpaceTimeSeriesTemplate(config, []any{}, time.Unix(0, 0), true); err != nil {
			return err
		} else if err = config.validateRotation(); err != nil {
			return err
		}
	}
	return nil
}

func (config *TimeSeriesConfig) validateRotation() error {
	if config.Rotation != "" {
		var err error
		config.rotationInterval, err = base.ParseLongDuration(config.Rotation)
		if err != nil {
			return err
		}
	} else {
		config.rotationInterval = kDefaultRotationInterval
	}
	if config.RotationMaxSize == 0 {
		config.RotationMaxSize = kDefaultRotationMaxSize
	} else if config.RotationMaxSize < 1000 || config.RotationMaxSize > 20_000_000 {
		return fmt.Errorf("invalid `rotation_max_size_bytes` %d", config.RotationMaxSize)
	}
	return nil
}

func validateSubscriptions(subs IngestMap) error {
	for topic, sub := range subs {
		if sub.Scope == "" {
			sub.Scope = base.DefaultScopeAndCollectionName().Scope
		}
		if sub.Collection == "" {
			sub.Collection = base.DefaultScopeAndCollectionName().Collection
		}
		if !sgbucket.IsValidDataStoreName(sub.Scope, sub.Collection) {
			return fmt.Errorf("invalid scope/collection names %q, %q in subscription %q",
				sub.Scope, sub.Collection, topic)
		}

		if _, err := MakeTopicFilter(topic); err != nil {
			return err
		}
		if sub.QoS != nil && (*sub.QoS < 0 || *sub.QoS > 2) {
			return fmt.Errorf("invalid `qos` value %v in subscription %q", *sub.QoS, topic)
		}
		if xform := sub.Encoding; xform != nil {
			if *xform != EncodingString && *xform != EncodingBase64 && *xform != EncodingJSON {
				return fmt.Errorf("invalid `transform` option %q in subscription %q", *xform, topic)
			}
		}

		if sub.Model != nil {
			switch *sub.Model {
			case ModelState:
				if err := validateStateTemplate(sub.StateTemplate); err != nil {
					return err
				} else if sub.TimeSeries != nil || sub.SpaceTimeSeries != nil {
					return fmt.Errorf("multiple model properties in subscription %q", topic)
				}
			case ModelTimeSeries:
				if err := sub.TimeSeries.Validate(); err != nil {
					return err
				} else if sub.StateTemplate != nil || sub.SpaceTimeSeries != nil {
					return fmt.Errorf("multiple model properties in subscription %q", topic)
				}
			case ModelSpaceTimeSeries:
				if err := sub.SpaceTimeSeries.Validate(); err != nil {
					return err
				} else if sub.StateTemplate != nil || sub.TimeSeries != nil {
					return fmt.Errorf("multiple model properties in subscription %q", topic)
				}
			default:
				return fmt.Errorf("invalid `model` %q in subscription %q", *sub.Model, topic)
			}
		} else if sub.TimeSeries == nil && sub.StateTemplate == nil {
			return fmt.Errorf("missing `model` subscription %q", topic)
		} else if sub.TimeSeries != nil && sub.StateTemplate != nil {
			return fmt.Errorf("cannot have both `state` and `time_series` in subscription %q", topic)
		}
	}
	return nil
}
