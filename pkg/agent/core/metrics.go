package core

// Definition of the Metrics type, plus reading it from vector.dev's prometheus format host metrics

import (
	"fmt"
	"io"

	promtypes "github.com/prometheus/client_model/go"
	promfmt "github.com/prometheus/common/expfmt"
	"github.com/tychoish/fun/erc"

	"github.com/neondatabase/autoscaling/pkg/api"
)

type SystemMetrics struct {
	LoadAverage1Min  float64
	MemoryUsageBytes float64
}

func (m SystemMetrics) ToAPI() api.Metrics {
	return api.Metrics{
		LoadAverage1Min:  float32(m.LoadAverage1Min),
		LoadAverage5Min:  nil,
		MemoryUsageBytes: nil,
	}
}

type SystemMetricNames struct {
	Load1        string `json:"load1"`
	Load5        string `json:"load5"`
	MemAvailable string `json:"memAvailable"`
	MemTotal     string `json:"memTotal"`
}

func (n SystemMetricNames) Validate() error {
	ec := &erc.Collector{}

	const emptyTmpl = "field %q cannot be empty"

	erc.Whenf(ec, n.Load1 == "", emptyTmpl, "load1")
	erc.Whenf(ec, n.Load5 == "", emptyTmpl, "load5")
	erc.Whenf(ec, n.MemAvailable == "", emptyTmpl, "memAvailable")
	erc.Whenf(ec, n.MemTotal == "", emptyTmpl, "memTotal")

	return ec.Resolve()
}

// FromPrometheus represents metric types that can be parsed from prometheus output.
//
// It is parameterized by the config 'C' passed to the parsing function, to allow things like metric
// names to be defined externally, rather than compile-time constants.
type FromPrometheus[C any] interface {
	fromPrometheus(C, map[string]*promtypes.MetricFamily) error
}

// ParseMetrics reads the prometheus text-format content, parses it, and uses M's implementation of
// FromPrometheus to populate it before returning.
func ParseMetrics[M FromPrometheus[C], C any](content io.Reader, config C, metrics M) error {
	var parser promfmt.TextParser
	mfs, err := parser.TextToMetricFamilies(content)
	if err != nil {
		return fmt.Errorf("failed to parse content as prometheus text format: %w", err)
	}

	if err := metrics.fromPrometheus(config, mfs); err != nil {
		return fmt.Errorf("failed to extract metrics: %w", err)
	}

	return nil
}

//nolint:unused // used by (*SystemMetrics).fromPrometheus()
func extractFloatGauge(mf *promtypes.MetricFamily) (float64, error) {
	if mf.GetType() != promtypes.MetricType_GAUGE {
		return 0, fmt.Errorf("wrong metric type: expected %s but got %s", promtypes.MetricType_GAUGE, mf.GetType())
	} else if len(mf.Metric) != 1 {
		return 0, fmt.Errorf("expected 1 metric, found %d", len(mf.Metric))
	}

	return mf.Metric[0].GetGauge().GetValue(), nil
}

// Helper function to return an error for a missing metric
//
//nolint:unused // used by (*SystemMetrics).fromPrometheus()
func missingMetric(name string) error {
	return fmt.Errorf("missing expected metric %s", name)
}

// fromPrometheus implements FromPrometheus, so SystemMetrics can be used with ParseMetrics.
//
//nolint:unused // https://github.com/dominikh/go-tools/issues/1294
func (m *SystemMetrics) fromPrometheus(names SystemMetricNames, mfs map[string]*promtypes.MetricFamily) error {
	ec := &erc.Collector{}

	getFloat := func(metricName string) float64 {
		if mf := mfs[metricName]; mf != nil {
			f, err := extractFloatGauge(mf)
			ec.Add(err) // does nothing if err == nil
			return f
		} else {
			ec.Add(missingMetric(metricName))
			return 0
		}
	}

	tmp := SystemMetrics{
		LoadAverage1Min: getFloat(names.Load1),
		// Add an extra 100 MiB to account for kernel memory usage
		MemoryUsageBytes: getFloat(names.MemTotal) - getFloat(names.MemAvailable) + 100*(1<<20),
	}

	if err := ec.Resolve(); err != nil {
		return err
	}

	*m = tmp
	return nil
}
