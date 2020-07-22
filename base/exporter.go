package base

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
)

type SgwStats struct {
	GlobalStats GlobalStat `json:"global_stats"`
	DbStats     map[string]DbStats
}

func (s SgwStats) String() string {
	bytes, _ := JSONMarshal(s)
	return string(bytes)
}

type GlobalStat struct {
	ResourceUtilization ResourceUtilization `json:"resource_utilization"`
}

type ResourceUtilization struct {
	AdminNetworkInterfaceBytesReceived  *sgwIntStat `json:"admin_network_interface_bytes_recv"`
	AdminNetworkInterfaceBytesSent      *sgwIntStat `json:"admin_network_interface_bytes_sent"`
	ErrorCount                          *sgwIntStat `json:"error_count"`
	GoMemstatsHeapAlloc                 *sgwIntStat `json:"go_memstats_heapalloc"`
	GoMemstatsHeapIdle                  *sgwIntStat `json:"go_memstats_heapidle"`
	GoMemstatsHeapInUse                 *sgwIntStat `json:"go_memstats_heapinuse"`
	GoMemstatsHeapReleased              *sgwIntStat `json:"go_memstats_heapreleased"`
	GoMemstatsPauseTotalNS              *sgwIntStat `json:"go_memstats_pausetotalns"`
	GoMemstatsStackInUse                *sgwIntStat `json:"go_memstats_stackinuse"`
	GoMemstatsStackSys                  *sgwIntStat `json:"go_memstats_stacksys"`
	GoMemstatsSys                       *sgwIntStat `json:"go_memstats_sys"`
	GoroutinesHighWatermark             *sgwIntStat `json:"goroutines_high_watermark"`
	NumGoroutines                       *sgwIntStat `json:"num_goroutines"`
	ProcessMemoryResident               *sgwIntStat `json:"process_memory_resident"`
	PublicNetworkInterfaceBytesReceived *sgwIntStat `json:"pub_net_bytes_recv"`
	PublicNetworkInterfaceBytesSent     *sgwIntStat `json:"pub_net_bytes_sent"`
	SystemMemoryTotal                   *sgwIntStat `json:"system_memory_total"`
	WarnCount                           *sgwIntStat `json:"warn_count"`
	CpuPercentUtil                      *sgwIntStat `json:"cpu_percent_util"`
}

type DbStats struct {
}

type sgwIntStat struct {
	statFQDN      string
	statDesc      *prometheus.Desc
	dbName        string
	statValueType prometheus.ValueType
	Value         interface{}
}

func NewIntStat(subsystem string, key string, dbName string, statValueType prometheus.ValueType, initialValue interface{}) *sgwIntStat {

	var label []string
	if dbName != "" {
		label = []string{"database"}
	}

	name := prometheus.BuildFQName("sgw", subsystem, key)
	desc := prometheus.NewDesc(name, key, label, nil)

	stat := &sgwIntStat{
		statFQDN:      name,
		statDesc:      desc,
		dbName:        dbName,
		statValueType: statValueType,
		Value:         initialValue,
	}

	prometheus.MustRegister(stat)

	return stat
}

func (s *sgwIntStat) Describe(ch chan<- *prometheus.Desc) {
	return
}

func (s *sgwIntStat) Collect(ch chan<- prometheus.Metric) {
	val := s.Value

	switch formattedVal := val.(type) {
	case int:
		if s.dbName != "" {
			ch <- prometheus.MustNewConstMetric(s.statDesc, s.statValueType, float64(formattedVal), s.dbName)
		} else {
			ch <- prometheus.MustNewConstMetric(s.statDesc, s.statValueType, float64(formattedVal))
		}
		break
	case uint64:
		if s.dbName != "" {
			ch <- prometheus.MustNewConstMetric(s.statDesc, s.statValueType, float64(formattedVal), s.dbName)
		} else {
			ch <- prometheus.MustNewConstMetric(s.statDesc, s.statValueType, float64(formattedVal))
		}
		break
	case float64:
		if s.dbName != "" {
			ch <- prometheus.MustNewConstMetric(s.statDesc, s.statValueType, formattedVal, s.dbName)
		} else {
			ch <- prometheus.MustNewConstMetric(s.statDesc, s.statValueType, formattedVal)
		}
		break
	default:
		panic("Err")
	}

}

func (s *sgwIntStat) Set(newV interface{}) {
	s.Value = newV
	fmt.Println(s.Value)
}

func (s *sgwIntStat) Add(newV int) {
	switch formattedVal := s.Value.(type) {
	case int:
		s.Value = formattedVal + newV
		break
	default:
		panic("Err")
	}
}

func (s *sgwIntStat) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%v", s.Value)), nil
}

func (s *sgwIntStat) String() string {
	return fmt.Sprintf("%v", s.Value)
}
