package db

import "expvar"

type BlipSyncStats struct {
	HandleRevCount            *expvar.Int
	HandleRevErrorCount       *expvar.Int
	SendRevCount              *expvar.Int
	SendRevErrorTotal         *expvar.Int
	SendRevErrorConflictCount *expvar.Int
	SendRevErrorRejectedCount *expvar.Int
	SendRevErrorOtherCount    *expvar.Int
	DocsPurgedCount           *expvar.Int
}

func NewBlipSyncStats() *BlipSyncStats {
	return &BlipSyncStats{
		HandleRevCount:            &expvar.Int{},
		HandleRevErrorCount:       &expvar.Int{},
		SendRevCount:              &expvar.Int{},
		SendRevErrorTotal:         &expvar.Int{},
		SendRevErrorConflictCount: &expvar.Int{},
		SendRevErrorRejectedCount: &expvar.Int{},
		SendRevErrorOtherCount:    &expvar.Int{},
		DocsPurgedCount:           &expvar.Int{},
	}
}
