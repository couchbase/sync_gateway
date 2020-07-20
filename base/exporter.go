package base

import (
	"expvar"

	"github.com/prometheus/client_golang/prometheus"
)

type Collector struct {
	DBName    string
	Subsystem string
	Info      map[string]StatComponents
	VarMap    *expvar.Map
}

type StatComponents struct {
	ValueType prometheus.ValueType
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	return
}

var perDbLabels = []string{"database"}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	c.VarMap.Do(func(value expvar.KeyValue) {
		var label []string

		if c.DBName != "" {
			label = perDbLabels
		}

		key := value.Key
		name := prometheus.BuildFQName("sgw", c.Subsystem, key)
		vType := c.Info[key].ValueType
		desc := prometheus.NewDesc(name, key, label, nil)

		if _, ok := c.Info[key]; ok {
			switch v := value.Value.(type) {
			case *expvar.Int:
				if c.DBName != "" {
					ch <- prometheus.MustNewConstMetric(desc, vType, float64(v.Value()), c.DBName)
				} else {
					ch <- prometheus.MustNewConstMetric(desc, vType, float64(v.Value()))
				}
				break
			case *expvar.Float:
				if c.DBName != "" {
					ch <- prometheus.MustNewConstMetric(desc, vType, v.Value(), c.DBName)
				} else {
					ch <- prometheus.MustNewConstMetric(desc, vType, v.Value())
				}
				break
			}
		}

	})
}
