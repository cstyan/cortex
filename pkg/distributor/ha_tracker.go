package distributor

import (
	"context"
	"fmt"
	"os"
	// "net/http"
	"sync"
	"time"

	"github.com/weaveworks/common/mtime"

	"github.com/cortexproject/cortex/pkg/ingester/client"
	"github.com/cortexproject/cortex/pkg/kvstore"
)

// Treat these as consts.
var haInstanceBytes = []byte("prom_ha_instance")
var haClusterBytes = []byte("prom_ha_cluster")

const prefix = "prom_ha"

// Track the most recent timestamp for the instance we're
// accepting samples from in a Prometheus HA setup.
type instanceTracker struct {
	instance  string
	timestamp int64
}

// Track the instance we're accepting samples from
// for each HA cluster we know about.
type tracker struct {
	// Instances we are accepting samples from.
	electedLock sync.RWMutex
	elected     map[string]instanceTracker

	// We should only update the timestamp this often
	writeTimeout int64

	consulLock       sync.RWMutex
	overwriteTimeout int64 // Timeout that has to occur before we overwrite to a different instance
	client           kvstore.KVClient
}

func NewClusterTracker(local bool) clusterTracker {
	var client kvstore.KVClient
	var err error
	codec := kvstore.ProtoCodec{Factory: ProtoInstanceDescFactory}
	if local {
		client = kvstore.NewInMemoryKVClient(codec)
	} else {
		client, err = kvstore.NewConsulClient(kvstore.ConsulConfig{
			Host: "localhost:8500",
		}, codec)

		if err != nil {
			os.Exit(1)
		}
	}
	return &tracker{
		elected:          make(map[string]instanceTracker),
		writeTimeout:     10 * time.Second.Nanoseconds(),
		overwriteTimeout: time.Minute.Nanoseconds(),
		client:           client,
	}
}

// This should take in a cancellable context eventually.
func (c *tracker) StartWatch() {
	go c.client.WatchPrefix(context.Background(), fmt.Sprintf("%s", prefix), func(value interface{}) bool {
		if value == nil {
			return true
		}
		return true
	})
}

func (c *tracker) setTimeout(overwrite, write int64) {
	c.overwriteTimeout = overwrite
	c.writeTimeout = write
}

// Wraps calls to client CAS with locking and error checking
// TODO, we probably want to return certain error types from the cas, to propagate back
// to callers so they can decide whether or not to cache info locally
func (c *tracker) casWrapper(ctx context.Context, cluster, instance string, now int64) error {
	err := c.client.CAS(context.Background(), fmt.Sprintf("%s/%s", prefix, cluster), func(in interface{}) (out interface{}, retry bool, err error) {
		desc, ok := in.(*InstanceDesc)
		// TODO: is this case right, what do we want to do?
		// it means there was either invalid or no data for the key
		if !ok || desc == nil {
			desc = &InstanceDesc{
				Instance:  instance,
				Timestamp: now,
			}
			return desc, false, nil
		}
		if now-desc.Timestamp > c.overwriteTimeout { // overwrite
			desc = &InstanceDesc{
				Instance:  instance,
				Timestamp: now,
			}
			return desc, false, nil
		}
		if desc.Instance != instance {
			return nil, false, fmt.Errorf("instances did not match, rejecting sample: %s != %s", instance, desc.Instance)
		}
		// This may only catch local CAS' updating Consul, not other distributors
		if desc.Timestamp+c.writeTimeout < now {
			desc = &InstanceDesc{
				Instance:  instance,
				Timestamp: now,
			}
			return desc, false, nil
		}
		// Here we just don't want to update what's in Consul so we return nil for the interface.
		return nil, false, nil
	})
	return err
}

// Do we want to actually do this all the time, or only do it when the write timeout is reached?
// Wraps updating the timestamp for an instance in the local cache so that we can grab the
// lock and then check if we should actually update it. The timestamp should only ever go up.
func (c *tracker) updateTimestamp(cluster, instance string, ts int64) {
	c.electedLock.Lock()
	defer c.electedLock.Unlock()
	v := c.elected[cluster]
	if v.timestamp < ts {
		c.elected[cluster] = instanceTracker{
			instance:  instance,
			timestamp: ts,
		}
	}
}

func (c *tracker) setInstance(cluster, instance string, ts int64) {
	c.electedLock.Lock()
	defer c.electedLock.Unlock()
	c.elected[cluster] = instanceTracker{
		instance:  instance,
		timestamp: ts,
	}
}

// TODO: simplify this function.
// Returns true if we should accept the sample for this cluster/instance, else false.
func (c *tracker) LookupInstance(cluster, instance string) *instanceTracker {
	now := mtime.Now().UnixNano()
	c.electedLock.RLock()
	v, ok := c.elected[cluster]
	c.electedLock.RUnlock()

	// No instance in local cache for this HA cluster yet.
	if !ok {
		return c.checkConsul(cluster, instance, now)
	}

	// The instance was in the distributors cache.
	return c.checkLocal(v, cluster, instance, now)
}

// Only calls return false to bail out early, otherwise continues on to return true at the end.
func (c *tracker) checkConsul(cluster, instance string, now int64) *instanceTracker {
	ret := instanceTracker{}

	// We should check Consul first, and only if it doesn't contain
	err := c.casWrapper(context.Background(), cluster, instance, now)
	if err != nil {
		return nil
	}
	c.setInstance(cluster, instance, now)
	c.electedLock.RLock()
	ret = c.elected[cluster]
	c.electedLock.RUnlock()
	return &ret
}

// We found an entry for the cluster in our local cache when doing a lookup.
func (c *tracker) checkLocal(entry instanceTracker, cluster, instance string, now int64) *instanceTracker {
	ret := instanceTracker{}
	if entry.instance != instance {
		// we don't want to overwrite yet
		if now-entry.timestamp < c.overwriteTimeout {
			return &entry
		}
		// overwrite
		err := c.casWrapper(context.Background(), cluster, instance, now)
		if err != nil {
			return nil
		}
		c.setInstance(cluster, instance, now)
		c.electedLock.RLock()
		ret = c.elected[cluster]
		c.electedLock.RUnlock()
		return &ret
	}
	// we should CAS the timestamp if writeTimeout
	if now-entry.timestamp > c.writeTimeout {
		err := c.casWrapper(context.Background(), cluster, instance, now)
		if err != nil {
			// how should we handle an error here?
			return nil
		}
		c.setInstance(cluster, instance, now)
	}
	c.electedLock.RLock()
	ret = c.elected[cluster]
	c.electedLock.RUnlock()
	return &ret
}

// The tracking is kind of ugly, fix later
// Modifies the labels parameter in place, removing labels that match
// the instance or cluster label and returning their values. Returns an error
// if we find one but not both of the labels.
func removeHALabels(labels *[]client.LabelPair) (string, string, error) {
	var cluster, instance string
	var err error
	var pair client.LabelPair

	foundOne := false

	for i := 0; i < len(*labels); i++ {
		pair = (*labels)[i]
		if pair.Name.Compare(haInstanceBytes) == 0 {
			instance = string(pair.Value)
			foundOne = true
			*labels = append((*labels)[:i], (*labels)[i+1:]...)
			i--
		} else if pair.Name.Compare(haClusterBytes) == 0 {
			cluster = string(pair.Value)
			foundOne = true
			*labels = append((*labels)[:i], (*labels)[i+1:]...)
			i--
		}
	}

	if (cluster == "" || instance == "") && foundOne {
		err = fmt.Errorf("found one HA label but not both, cluster: %s instance: %s", cluster, instance)
	}
	return cluster, instance, err
}
