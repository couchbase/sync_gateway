package base

import (
	"expvar"
	"time"
)

var StatsExpvars *expvar.Map = expvar.NewMap("syncGateway_stats")
var TimingExpvars SequenceTimingExpvar
var perDbStatNames = []string{
	"changesFeeds_active",
	"changesFeeds_total",
	"requests_active",
	"requests_total",
	"revisionCache_hits",
	"revisionCache_misses",
	"simpleChanges_active",
	"simpleChanges_total",
	"subChanges_active",
	"subChanges_total",
	"vectorChanges_active",
	"vectorChanges_total"}
var globalStatNames = append(perDbStatNames, []string{
	"bulkApi.BulkDocsPerDocRollingMean",
	"bulkApi.BulkDocsRollingMean",
	"bulkApi.BulkGetPerDocRollingMean",
	"bulkApi.BulkGetRollingMean",
	"goroutines_highWaterMark",
	"handler.CheckAuthRollingMean",
	"indexReader.getChanges.Count",
	"indexReader.getChanges.Time",
	"indexReader.getChanges.UseCached",
	"indexReader.getChanges.UseIndexed",
	"indexReader.numReaders.OneShot",
	"indexReader.numReaders.Persistent",
	"indexReader.pollPrincipals.Count",
	"indexReader.pollPrincipals.Time",
	"indexReader.pollReaders.Count",
	"indexReader.pollReaders.Time",
	"indexReader.seqHasher.GetClockTime",
	"indexReader.seqHasher.GetHash"}...)

const (
	KTimingExpvarVbNo      = 0
	KTimingExpvarFrequency = 200
)

//initialise the expvar properties that are exposed via syncGateway_stats
//this ensures the properties are published even if there are no metrics
//generated. This is useful when building dashboards that query against data
//to find valid property names
//
func init() {
	for _, stat := range globalStatNames {
		StatsExpvars.Add(stat, 0)
	}
	TimingExpvars = NewSequenceTimingExpvar(KTimingExpvarFrequency, KTimingExpvarVbNo, "st")
	StatsExpvars.Set("sequenceTiming", TimingExpvars)
	InitDbStats("_server")
}

func InitDbStats(name string) {
	if StatsExpvars.Get(name) == nil {
		dbStats := expvar.NewMap(name)
		for _, stat := range perDbStatNames {
			dbStats.Add(stat, 0)
		}
		StatsExpvars.Set(name, dbStats)
	}
}

func UpdateDbStat(dbName, statName string, delta int64) {
	if dbName != "" {
		dbStats := StatsExpvars.Get(dbName)
		if s, ok := dbStats.(*expvar.Map); ok {
			s.Add(statName, delta)
		}
	}
	// Update the global stat. Do this even if it doesn't correspond to a db for backwards compatibility
	StatsExpvars.Add(statName, delta)
}
