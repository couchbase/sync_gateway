package db

import "expvar"

type BlipSyncStats struct {
	HandleRevCount      *expvar.Int
	HandleRevErrorCount *expvar.Int
	SendRevCount        *expvar.Int
	SendRevErrorCount   *expvar.Int
	DocsPurgedCount     *expvar.Int
}

func NewBlipSyncStats() *BlipSyncStats {
	return &BlipSyncStats{
		HandleRevCount:      &expvar.Int{},
		HandleRevErrorCount: &expvar.Int{},
		SendRevCount:        &expvar.Int{},
		SendRevErrorCount:   &expvar.Int{},
		DocsPurgedCount:     &expvar.Int{},
	}
}
