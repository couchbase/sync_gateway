package base

var TimingExpvars SequenceTimingExpvar

// The peak number of goroutines observed during lifetime of program
var MaxGoroutinesSeen uint64

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

	// Sg-Accel stats -- this should be in db.IndexExpvars, but cannot be due to dependency it would create from base -> db
	TimingExpvars = NewSequenceTimingExpvar(KTimingExpvarFrequency, KTimingExpvarVbNo, "st")
	ShardedClockExpvars.Set("sequenceTiming", &TimingExpvars)

}
