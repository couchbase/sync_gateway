package base

import "expvar"

var StatsExpvars *expvar.Map = expvar.NewMap("syncGateway_stats")
var TimingExpvars SequenceTimingExpvar

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
	StatsExpvars.Add("changesFeeds_total", 0)
	StatsExpvars.Add("changesFeeds_active", 0)
	StatsExpvars.Add("requests_total", 0)
	StatsExpvars.Add("requests_active", 0)
	StatsExpvars.Add("revisionCache_hits", 0)
	StatsExpvars.Add("revisionCache_misses", 0)
	TimingExpvars = NewSequenceTimingExpvar(KTimingExpvarFrequency, KTimingExpvarVbNo, "st")
	StatsExpvars.Set("sequenceTiming", TimingExpvars)

}
