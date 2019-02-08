package kvstore

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"time"

	"github.com/go-kit/kit/log/level"
	consul_api "github.com/hashicorp/consul/api"
	cleanhttp "github.com/hashicorp/go-cleanhttp"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/weaveworks/common/instrument"
)

const (
	longPollDuration = 10 * time.Second
)

// ConsulConfig to create a ConsulClient
type ConsulConfig struct {
	Host              string
	Prefix            string
	ACLToken          string
	HTTPClientTimeout time.Duration
	ConsistentReads   bool
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *ConsulConfig) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.Host, "consul.hostname", "localhost:8500", "Hostname and port of Consul.")
	f.StringVar(&cfg.Prefix, "consul.prefix", "collectors/", "Prefix for keys in Consul.")
	f.StringVar(&cfg.ACLToken, "consul.acltoken", "", "ACL Token used to interact with Consul.")
	f.DurationVar(&cfg.HTTPClientTimeout, "consul.client-timeout", 2*longPollDuration, "HTTP timeout when talking to consul")
	f.BoolVar(&cfg.ConsistentReads, "consul.consistent-reads", true, "Enable consistent reads to consul.")
}

type kv interface {
	CAS(p *consul_api.KVPair, q *consul_api.WriteOptions) (bool, *consul_api.WriteMeta, error)
	Get(key string, q *consul_api.QueryOptions) (*consul_api.KVPair, *consul_api.QueryMeta, error)
	Keys(prefix, separator string, q *consul_api.QueryOptions) ([]string, *consul_api.QueryMeta, error)
	List(path string, q *consul_api.QueryOptions) (consul_api.KVPairs, *consul_api.QueryMeta, error)
	Put(p *consul_api.KVPair, q *consul_api.WriteOptions) (*consul_api.WriteMeta, error)
}

type ConsulClient struct {
	kv
	codec Codec
	cfg   ConsulConfig
}

// NewConsulClient returns a new ConsulClient.
func NewConsulClient(cfg ConsulConfig, codec Codec) (KVClient, error) {
	client, err := consul_api.NewClient(&consul_api.Config{
		Address: cfg.Host,
		Token:   cfg.ACLToken,
		Scheme:  "http",
		HttpClient: &http.Client{
			Transport: cleanhttp.DefaultPooledTransport(),
			// See https://blog.cloudflare.com/the-complete-guide-to-golang-net-http-timeouts/
			Timeout: cfg.HTTPClientTimeout,
		},
	})
	if err != nil {
		return nil, err
	}
	var c KVClient = &ConsulClient{
		kv:    consulMetrics{client.KV()},
		codec: codec,
		cfg:   cfg,
	}
	if cfg.Prefix != "" {
		c = PrefixClient(c, cfg.Prefix)
	}
	return c, nil
}

var (
	writeOptions = &consul_api.WriteOptions{}

	// ErrNotFound is returned by ConsulClient.Get.
	ErrNotFound = fmt.Errorf("Not found")
)

// CAS atomically modifies a value in a callback.
// If value doesn't exist you'll get nil as an argument to your callback.
func (c *ConsulClient) CAS(ctx context.Context, key string, f CASCallback) error {
	return instrument.CollectedRequest(ctx, "CAS loop", consulRequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		return c.cas(ctx, key, f)
	})
}

func (c *ConsulClient) cas(ctx context.Context, key string, f CASCallback) error {
	var (
		index   = uint64(0)
		retries = 10
		retry   = true
	)
	for i := 0; i < retries; i++ {
		options := &consul_api.QueryOptions{
			RequireConsistent: c.cfg.ConsistentReads,
		}
		kvp, _, err := c.kv.Get(key, options.WithContext(ctx))
		if err != nil {
			level.Error(util.Logger).Log("msg", "error getting key", "key", key, "err", err)
			continue
		}
		var intermediate interface{}
		if kvp != nil {
			out, err := c.codec.Decode(kvp.Value)
			if err != nil {
				level.Error(util.Logger).Log("msg", "error decoding key", "key", key, "err", err)
				continue
			}
			// If key doesn't exist, index will be 0.
			index = kvp.ModifyIndex
			intermediate = out
		}

		intermediate, retry, err = f(intermediate)
		if err != nil {
			level.Error(util.Logger).Log("msg", "error CASing", "key", key, "err", err)
			if !retry {
				return err
			}
			continue
		}

		if intermediate == nil {
			level.Info(util.Logger).Log("msg", "CAS callback returned nil, not writing to kvstore")
			return fmt.Errorf("callback returned nil, not writing to kvstore")
			// panic("Callback must instantiate value!")
		}
		bytes, err := c.codec.Encode(intermediate)
		if err != nil {
			level.Error(util.Logger).Log("msg", "error serialising value", "key", key, "err", err)
			continue
		}
		ok, _, err := c.kv.CAS(&consul_api.KVPair{
			Key:         key,
			Value:       bytes,
			ModifyIndex: index,
		}, writeOptions.WithContext(ctx))
		if err != nil {
			level.Error(util.Logger).Log("msg", "error CASing", "key", key, "err", err)
			continue
		}
		if !ok {
			level.Debug(util.Logger).Log("msg", "error CASing, trying again", "key", key, "index", index)
			continue
		}
		return nil
	}
	return fmt.Errorf("failed to CAS %s", key)
}

var backoffConfig = util.BackoffConfig{
	MinBackoff: 1 * time.Second,
	MaxBackoff: 1 * time.Minute,
}

// WatchKey will watch a given key in consul for changes. When the value
// under said key changes, the f callback is called with the deserialised
// value. To construct the deserialised value, a factory function should be
// supplied which generates an empty struct for WatchKey to deserialise
// into. Values in Consul are assumed to be JSON. This function blocks until
// the context is cancelled.
func (c *ConsulClient) WatchKey(ctx context.Context, key string, f func(interface{}) bool) {
	var (
		backoff = util.NewBackoff(ctx, backoffConfig)
		index   = uint64(0)
	)
	for backoff.Ongoing() {
		queryOptions := &consul_api.QueryOptions{
			RequireConsistent: true,
			WaitIndex:         index,
			WaitTime:          longPollDuration,
		}

		kvp, meta, err := c.kv.Get(key, queryOptions.WithContext(ctx))
		if err != nil || kvp == nil {
			level.Error(util.Logger).Log("msg", "error getting path", "key", key, "err", err)
			backoff.Wait()
			continue
		}
		backoff.Reset()
		// Skip if the index is the same as last time, because the key value is
		// guaranteed to be the same as last time
		if index == meta.LastIndex {
			continue
		}
		index = meta.LastIndex

		out, err := c.codec.Decode(kvp.Value)
		if err != nil {
			level.Error(util.Logger).Log("msg", "error decoding key", "key", key, "err", err)
			continue
		}
		if !f(out) {
			return
		}
	}
}

// Noop for now
func (c *ConsulClient) WatchPrefix(ctx context.Context, prefix string, f func(interface{}) bool) {
	var (
		backoff = util.NewBackoff(ctx, backoffConfig)
		index   = uint64(0)
	)
	for backoff.Ongoing() {
		queryOptions := &consul_api.QueryOptions{
			RequireConsistent: true,
			WaitIndex:         index,
			WaitTime:          longPollDuration,
		}

		kvps, meta, err := c.kv.List(prefix, queryOptions.WithContext(ctx))
		if err != nil || kvps == nil {
			level.Error(util.Logger).Log("msg", "error getting path", "prefix", prefix, "err", err)
			backoff.Wait()
			continue
		}
		backoff.Reset()
		// Skip if the index is the same as last time, because the key value is
		// guaranteed to be the same as last time
		if index == meta.LastIndex {
			continue
		}
		index = meta.LastIndex
		// We need a byte array
		var buf []byte
		for _, kvp := range kvps {
			buf = append(buf, kvp.Value...)
		}
		out, err := c.codec.Decode(buf)
		if err != nil {
			level.Error(util.Logger).Log("msg", "error decoding list of values for prefix", "prefix", prefix, "err", err)
			continue
		}
		if !f(out) {
			return
		}
	}
}

func (c *ConsulClient) PutBytes(ctx context.Context, key string, buf []byte) error {
	_, err := c.kv.Put(&consul_api.KVPair{
		Key:   key,
		Value: buf,
	}, writeOptions.WithContext(ctx))
	return err
}

func (c *ConsulClient) Get(ctx context.Context, key string) (interface{}, error) {
	options := &consul_api.QueryOptions{
		RequireConsistent: c.cfg.ConsistentReads,
	}
	kvp, _, err := c.kv.Get(key, options.WithContext(ctx))
	if err != nil {
		return nil, err
	} else if kvp == nil {
		return nil, nil
	}
	return c.codec.Decode(kvp.Value)
}

type prefixedConsulClient struct {
	prefix string
	consul KVClient
}

// PrefixClient takes a ConsulClient and forces a prefix on all its operations.
func PrefixClient(client KVClient, prefix string) KVClient {
	return &prefixedConsulClient{prefix, client}
}

// CAS atomically modifies a value in a callback. If the value doesn't exist,
// you'll get 'nil' as an argument to your callback.
func (c *prefixedConsulClient) CAS(ctx context.Context, key string, f CASCallback) error {
	return c.consul.CAS(ctx, c.prefix+key, f)
}

// WatchKey watches a key.
func (c *prefixedConsulClient) WatchKey(ctx context.Context, key string, f func(interface{}) bool) {
	c.consul.WatchKey(ctx, c.prefix+key, f)
}

// WatchPrefix Watches all keys under the given prefix.
func (c *prefixedConsulClient) WatchPrefix(ctx context.Context, prefix string, f func(interface{}) bool) {
	c.consul.WatchKey(ctx, prefix, f)
}

// PutBytes writes bytes to Consul.
func (c *prefixedConsulClient) PutBytes(ctx context.Context, key string, buf []byte) error {
	return c.consul.PutBytes(ctx, c.prefix+key, buf)
}

func (c *prefixedConsulClient) Get(ctx context.Context, key string) (interface{}, error) {
	return c.consul.Get(ctx, c.prefix+key)
}
