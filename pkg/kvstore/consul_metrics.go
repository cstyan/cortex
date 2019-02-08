package kvstore

import (
	"context"

	consul_api "github.com/hashicorp/consul/api"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/weaveworks/common/instrument"
)

var consulRequestDuration = instrument.NewHistogramCollector(prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "cortex",
	Name:      "consul_request_duration_seconds",
	Help:      "Time spent on consul requests.",
	Buckets:   prometheus.DefBuckets,
}, []string{"operation", "status_code"}))

// func init() {
// 	consulRequestDuration.Register()
// }

type consulMetrics struct {
	kv
}

func (c consulMetrics) CAS(p *consul_api.KVPair, options *consul_api.WriteOptions) (bool, *consul_api.WriteMeta, error) {
	var ok bool
	var result *consul_api.WriteMeta
	err := instrument.CollectedRequest(options.Context(), "CAS", consulRequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		options = options.WithContext(ctx)
		var err error
		ok, result, err = c.kv.CAS(p, options)
		return err
	})
	return ok, result, err
}

func (c consulMetrics) Get(key string, options *consul_api.QueryOptions) (*consul_api.KVPair, *consul_api.QueryMeta, error) {
	var kvp *consul_api.KVPair
	var meta *consul_api.QueryMeta
	err := instrument.CollectedRequest(options.Context(), "Get", consulRequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		options = options.WithContext(ctx)
		var err error
		kvp, meta, err = c.kv.Get(key, options)
		return err
	})
	return kvp, meta, err
}

func (c consulMetrics) List(path string, options *consul_api.QueryOptions) (consul_api.KVPairs, *consul_api.QueryMeta, error) {
	var kvps consul_api.KVPairs
	var meta *consul_api.QueryMeta
	err := instrument.CollectedRequest(options.Context(), "List", consulRequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		options = options.WithContext(ctx)
		var err error
		kvps, meta, err = c.kv.List(path, options)
		return err
	})
	return kvps, meta, err
}

func (c consulMetrics) Put(p *consul_api.KVPair, options *consul_api.WriteOptions) (*consul_api.WriteMeta, error) {
	var result *consul_api.WriteMeta
	err := instrument.CollectedRequest(options.Context(), "Put", consulRequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		options = options.WithContext(ctx)
		var err error
		result, err = c.kv.Put(p, options)
		return err
	})
	return result, err
}
