package kvstore

import (
	fmt "fmt"
	"sync"
	"time"

	"github.com/cortexproject/cortex/pkg/util"
	"github.com/go-kit/kit/log/level"
	consul_api "github.com/hashicorp/consul/api"
)

type mockKV struct {
	mtx     sync.Mutex
	cond    *sync.Cond
	kvps    map[string]*consul_api.KVPair
	current uint64 // the current 'index in the log'
}

// NewInMemoryKVClient makes a new mock consul client.
func NewInMemoryKVClient(codec Codec) KVClient {
	m := mockKV{
		kvps: map[string]*consul_api.KVPair{},
	}
	m.cond = sync.NewCond(&m.mtx)
	go m.loop()
	return &ConsulClient{
		kv:    &m,
		codec: codec,
	}
}

func copyKVPair(in *consul_api.KVPair) *consul_api.KVPair {
	out := *in
	out.Value = make([]byte, len(in.Value))
	copy(out.Value, in.Value)
	return &out
}

// periodic loop to wake people up, so they can honour timeouts
func (m *mockKV) loop() {
	for range time.Tick(1 * time.Second) {
		m.mtx.Lock()
		m.cond.Broadcast()
		m.mtx.Unlock()
	}
}

func (m *mockKV) Put(p *consul_api.KVPair, q *consul_api.WriteOptions) (*consul_api.WriteMeta, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	m.current++
	existing, ok := m.kvps[p.Key]
	if ok {
		existing.Value = p.Value
		existing.ModifyIndex = m.current
	} else {
		m.kvps[p.Key] = &consul_api.KVPair{
			Key:         p.Key,
			Value:       p.Value,
			CreateIndex: m.current,
			ModifyIndex: m.current,
		}
	}

	m.cond.Broadcast()
	return nil, nil
}

func (m *mockKV) CAS(p *consul_api.KVPair, q *consul_api.WriteOptions) (bool, *consul_api.WriteMeta, error) {
	level.Debug(util.Logger).Log("msg", "CAS", "key", p.Key, "modify_index", p.ModifyIndex, "value", fmt.Sprintf("%.40q", p.Value))
	m.mtx.Lock()
	defer m.mtx.Unlock()
	existing, ok := m.kvps[p.Key]
	if ok && existing.ModifyIndex != p.ModifyIndex {
		return false, nil, nil
	}

	m.current++
	if ok {
		existing.Value = p.Value
		existing.ModifyIndex = m.current
	} else {
		m.kvps[p.Key] = &consul_api.KVPair{
			Key:         p.Key,
			Value:       p.Value,
			CreateIndex: m.current,
			ModifyIndex: m.current,
		}
	}

	m.cond.Broadcast()
	return true, nil, nil
}

func (m *mockKV) Get(key string, q *consul_api.QueryOptions) (*consul_api.KVPair, *consul_api.QueryMeta, error) {
	level.Debug(util.Logger).Log("msg", "Get", "key", key, "wait_index", q.WaitIndex)
	m.mtx.Lock()
	defer m.mtx.Unlock()

	value, ok := m.kvps[key]
	if !ok {
		level.Debug(util.Logger).Log("msg", "Get - not found", "key", key)
		return nil, &consul_api.QueryMeta{LastIndex: m.current}, nil
	}

	if q.WaitTime > 0 {
		deadline := time.Now().Add(q.WaitTime)
		for q.WaitIndex >= value.ModifyIndex && time.Now().Before(deadline) {
			m.cond.Wait()
		}
		if time.Now().After(deadline) {
			level.Debug(util.Logger).Log("msg", "Get - deadline exceeded", "key", key)
			return nil, &consul_api.QueryMeta{LastIndex: q.WaitIndex}, nil
		}
	}

	level.Debug(util.Logger).Log("msg", "Get", "key", key, "modify_index", value.ModifyIndex, "value", fmt.Sprintf("%.40q", value.Value))
	return copyKVPair(value), &consul_api.QueryMeta{LastIndex: value.ModifyIndex}, nil
}

func (m *mockKV) Keys(prefix, separator string, q *consul_api.QueryOptions) ([]string, *consul_api.QueryMeta, error) {
	//noop
	return nil, nil, nil
}

func (m *mockKV) List(prefix string, q *consul_api.QueryOptions) (consul_api.KVPairs, *consul_api.QueryMeta, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	deadline := time.Now().Add(q.WaitTime)
	for q.WaitIndex >= m.current && time.Now().Before(deadline) {
		m.cond.Wait()
	}
	if time.Now().After(deadline) {
		return nil, &consul_api.QueryMeta{LastIndex: q.WaitIndex}, nil
	}

	result := consul_api.KVPairs{}
	for _, kvp := range m.kvps {
		if kvp.ModifyIndex >= q.WaitIndex {
			result = append(result, copyKVPair(kvp))
		}
	}
	return result, &consul_api.QueryMeta{LastIndex: m.current}, nil
}
