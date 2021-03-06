package validation

import (
	"net/http"
	"time"

	"flag"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/weaveworks/common/httpgrpc"
	"github.com/weaveworks/cortex/pkg/ingester/client"
	"github.com/weaveworks/cortex/pkg/util/extract"
)

const (
	discardReasonLabel = "reason"

	errMissingMetricName = "sample missing metric name"
	errInvalidMetricName = "sample invalid metric name: %.200q"
	errInvalidLabel      = "sample invalid label: %.200q metric %.200q"
	errLabelNameTooLong  = "label name too long: %.200q metric %.200q"
	errLabelValueTooLong = "label value too long: %.200q metric %.200q"
	errTooManyLabels     = "sample for '%s' has %d label names; limit %d"
	errTooOld            = "sample for '%s' has timestamp too old: %d"
	errTooNew            = "sample for '%s' has timestamp too new: %d"

	greaterThanMaxSampleAge = "greater_than_max_sample_age"
	maxLabelNamesPerSeries  = "max_label_names_per_series"
	tooFarInFuture          = "too_far_in_future"
	invalidLabel            = "label_invalid"
	labelNameTooLong        = "label_name_too_long"
	labelValueTooLong       = "label_value_too_long"
)

// DiscardedSamples is a metric of the number of discarded samples, by reason.
var DiscardedSamples = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "cortex_discarded_samples_total",
		Help: "The total number of samples that were discarded.",
	},
	[]string{discardReasonLabel, "user"},
)

func init() {
	prometheus.MustRegister(DiscardedSamples)
}

// Config for validation settings and options.
type Config struct {
	// maximum length a label name can be
	MaxLabelNameLength int

	// maximum length a label value can be. This also is the maximum length of a metric name.
	MaxLabelValueLength int

	// maximum number of label/value pairs timeseries.
	MaxLabelNamesPerSeries int

	// Config for rejecting old samples.
	RejectOldSamples       bool
	RejectOldSamplesMaxAge time.Duration
	CreationGracePeriod    time.Duration
}

// RegisterFlags registers a set of command line flags for setting options regarding sample validation at ingestion time.
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.MaxLabelNameLength, "validation.max-length-label-name", 1024, "Maximum length accepted for label names")
	f.IntVar(&cfg.MaxLabelValueLength, "validation.max-length-label-value", 2048, "Maximum length accepted for label value. This setting also applies to the metric name")
	f.IntVar(&cfg.MaxLabelNamesPerSeries, "validation.max-label-names-per-series", 30, "Maximum number of label names per series.")
	f.BoolVar(&cfg.RejectOldSamples, "validation.reject-old-samples", false, "Reject old samples.")
	f.DurationVar(&cfg.RejectOldSamplesMaxAge, "validation.reject-old-samples.max-age", 14*24*time.Hour, "Maximum accepted sample age before rejecting.")
	f.DurationVar(&cfg.CreationGracePeriod, "validation.create-grace-period", 10*time.Minute, "Duration which table will be created/deleted before/after it's needed; we won't accept sample from before this time.")
}

// ValidateSample returns an err if the sample is invalid.
func (cfg *Config) ValidateSample(userID string, metricName []byte, s client.Sample) error {
	if cfg.RejectOldSamples && model.Time(s.TimestampMs) < model.Now().Add(-cfg.RejectOldSamplesMaxAge) {
		DiscardedSamples.WithLabelValues(greaterThanMaxSampleAge, userID).Inc()
		return httpgrpc.Errorf(http.StatusBadRequest, errTooOld, metricName, model.Time(s.TimestampMs))
	}

	if model.Time(s.TimestampMs) > model.Now().Add(cfg.CreationGracePeriod) {
		DiscardedSamples.WithLabelValues(tooFarInFuture, userID).Inc()
		return httpgrpc.Errorf(http.StatusBadRequest, errTooNew, metricName, model.Time(s.TimestampMs))
	}

	return nil
}

// ValidateLabels returns an err if the labels are invalid.
func (cfg *Config) ValidateLabels(userID string, ls []client.LabelPair) error {
	metricName, err := extract.MetricNameFromLabelPairs(ls)
	if err != nil {
		return httpgrpc.Errorf(http.StatusBadRequest, errMissingMetricName)
	}

	if !model.IsValidMetricName(model.LabelValue(metricName)) {
		return httpgrpc.Errorf(http.StatusBadRequest, errInvalidMetricName, metricName)
	}

	numLabelNames := len(ls)
	if numLabelNames > cfg.MaxLabelNamesPerSeries {
		DiscardedSamples.WithLabelValues(maxLabelNamesPerSeries, userID).Inc()
		return httpgrpc.Errorf(http.StatusBadRequest, errTooManyLabels, metricName, numLabelNames, cfg.MaxLabelNamesPerSeries)
	}

	for _, l := range ls {
		var errTemplate string
		var reason string
		var cause interface{}
		if !model.LabelName(l.Name).IsValid() {
			reason = invalidLabel
			errTemplate = errInvalidLabel
			cause = l.Name
		} else if len(l.Name) > cfg.MaxLabelNameLength {
			reason = labelNameTooLong
			errTemplate = errLabelNameTooLong
			cause = l.Name
		} else if len(l.Value) > cfg.MaxLabelValueLength {
			reason = labelValueTooLong
			errTemplate = errLabelValueTooLong
			cause = l.Value
		}
		if errTemplate != "" {
			DiscardedSamples.WithLabelValues(reason, userID).Inc()
			return httpgrpc.Errorf(http.StatusBadRequest, errTemplate, cause, metricName)
		}
	}
	return nil
}
