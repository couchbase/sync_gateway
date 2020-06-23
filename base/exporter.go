package base

import (
	"expvar"

	"github.com/prometheus/client_golang/prometheus"
)

type Collector struct {
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

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	c.VarMap.Do(func(value expvar.KeyValue) {
		key := value.Key
		name := prometheus.BuildFQName("sgw", c.Subsystem, key)
		vType := c.Info[key].ValueType
		desc := prometheus.NewDesc(name, key, nil, nil)

		if _, ok := c.Info[key]; ok {
			switch v := value.Value.(type) {
			case *expvar.Int:
				ch <- prometheus.MustNewConstMetric(desc, vType, float64(v.Value()))
				break
			case *expvar.Float:
				ch <- prometheus.MustNewConstMetric(desc, vType, v.Value())
				break
			}
		}

	})
}
