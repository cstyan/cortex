package distributor

import (
	"github.com/cortexproject/cortex/pkg/ring"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/weaveworks/common/mtime"

	"github.com/cortexproject/cortex/pkg/ingester/client"
)

func TestLookupInstance_OverwriteTimeout(t *testing.T) {
	start := mtime.Now()
	c := NewClusterTracker(true, ring.ConsulConfig{})
	c.setTimeout(time.Second.Nanoseconds(), time.Second.Nanoseconds())

	// Write the first time.
	ret := c.LookupInstance("test", "instance1")
	assert.Equal(t, ret.instance, "instance1")

	// Throw away a sample from instance2
	ret = c.LookupInstance("test", "instance2")
	assert.NotEqual(t, ret.instance, "instance2")
	// assert.Equal(t, false, ok)

	// Wait more than the overwrite timeout.
	mtime.NowForce(start.Add(1100 * time.Millisecond))

	// Accept from instance 2, this should overwrite the saved instance of instance 1
	ret = c.LookupInstance("test", "instance2")
	assert.Equal(t, ret.instance, "instance2")

	// We timed out accepting samples from instance 1 and should now reject them.
	ret = c.LookupInstance("test", "instance1")
	assert.NotEqual(t, ret.instance, "instance1")
}

func TestLookupInstance_MultiCluster(t *testing.T) {
	c := NewClusterTracker(true, ring.ConsulConfig{})
	instance1 := "instance1"
	instance2 := "instance2"

	// Write the first time.
	ret := c.LookupInstance("c1", instance1)
	assert.Equal(t, ret.instance, instance1)
	ret = c.LookupInstance("c2", instance1)
	assert.Equal(t, ret.instance, instance1)

	// Reject samples from instance 2 in each cluster.
	ret = c.LookupInstance("c1", instance2)
	assert.NotEqual(t, ret.instance, instance2)
	ret = c.LookupInstance("c2", "instance2")
	assert.NotEqual(t, ret.instance, instance2)

	// We should still accept from instance 1.
	ret = c.LookupInstance("c1", "instance1")
	assert.Equal(t, ret.instance, instance1)
	ret = c.LookupInstance("c2", "instance1")
	assert.Equal(t, ret.instance, instance1)
}

func TestLookupInstance_MultiCluster_Timeout(t *testing.T) {
	start := mtime.Now()
	instance1 := "instance1"
	instance2 := "instance2"
	c := NewClusterTracker(true, ring.ConsulConfig{})
	c.setTimeout(time.Second.Nanoseconds(), time.Second.Nanoseconds())

	// Write the first time.
	ret := c.LookupInstance("c1", instance1)
	assert.Equal(t, ret.instance, instance1)
	ret = c.LookupInstance("c2", instance1)
	assert.Equal(t, ret.instance, instance1)

	// Reject samples from instance 2 in each cluster.
	ret = c.LookupInstance("c1", "instance2")
	assert.NotEqual(t, ret.instance, instance2)
	ret = c.LookupInstance("c2", "instance2")
	assert.NotEqual(t, ret.instance, instance2)

	// Accept a sample for instance 1 in C2.
	// go func() {
	// ticker := time.NewTicker(500 * time.Millisecond)
	// <-ticker.C
	mtime.NowForce(start.Add(500 * time.Millisecond))

	ret = c.LookupInstance("c2", "instance1")
	assert.Equal(t, ret.instance, instance1)
	// }()

	// Wait more than the timeout.
	mtime.NowForce(start.Add(1100 * time.Millisecond))

	// Accept a sample from c1/instance2
	ret = c.LookupInstance("c1", instance2)
	assert.Equal(t, ret.instance, instance2)

	// We should still accept from c2/instance1 but reject from c1/instance1.
	ret = c.LookupInstance("c1", instance1)
	assert.NotEqual(t, ret.instance, instance1)
	ret = c.LookupInstance("c2", instance1)
	assert.Equal(t, ret.instance, instance1)
}

// test that writes only happen every write timeout
func TestLookupInstance_WriteTimeout(t *testing.T) {
	start := mtime.Now()
	instance1 := "instance1"
	// instance2 := "instance2"
	c := NewClusterTracker(true, ring.ConsulConfig{})
	c.setTimeout(2*time.Second.Nanoseconds(), time.Second.Nanoseconds())

	// Write the first time.
	ret := c.LookupInstance("test", instance1)
	assert.Equal(t, ret.instance, instance1)
	tsSave := ret.timestamp

	// Timestamp should not update here.
	ret = c.LookupInstance("test", instance1)
	assert.Equal(t, ret.instance, instance1)
	assert.Equal(t, ret.timestamp, tsSave)

	// Wait 500ms and the timestamp should still not update.
	mtime.NowForce(start.Add(500 * time.Millisecond))
	ret = c.LookupInstance("test", instance1)
	assert.Equal(t, ret.instance, instance1)
	assert.Equal(t, ret.timestamp, tsSave)

	// Now we've waited > 1s, so the timestamp should update.
	mtime.NowForce(start.Add(1100 * time.Millisecond))
	ret = c.LookupInstance("test", instance1)
	assert.Equal(t, ret.instance, instance1)
	assert.NotEqual(t, ret.timestamp, tsSave)

}

func TestRemoveHALabels(t *testing.T) {
	type expectedOutput struct {
		cluster  string
		instance string
		err      bool
	}
	cases := []struct {
		labelsIn []client.LabelPair
		expected expectedOutput
	}{
		{
			[]client.LabelPair{
				{Name: []byte("__name__"), Value: []byte("foo")},
				{Name: []byte("bar"), Value: []byte("baz")},
				{Name: []byte("sample"), Value: []byte("1")},
				{Name: []byte(haInstanceBytes), Value: []byte("1")},
			},
			expectedOutput{cluster: "", instance: "1", err: true},
		},
		{
			[]client.LabelPair{
				{Name: []byte("__name__"), Value: []byte("foo")},
				{Name: []byte("bar"), Value: []byte("baz")},
				{Name: []byte("sample"), Value: []byte("2")},
				{Name: []byte(haClusterBytes), Value: []byte("cluster-2")},
			},
			expectedOutput{cluster: "cluster-2", instance: "", err: true},
		},
		{
			[]client.LabelPair{
				{Name: []byte("__name__"), Value: []byte("foo")},
				{Name: []byte("bar"), Value: []byte("baz")},
				{Name: []byte("sample"), Value: []byte("3")},
				{Name: []byte(haInstanceBytes), Value: []byte("3")},
				{Name: []byte(haClusterBytes), Value: []byte("cluster-3")},
			},
			expectedOutput{cluster: "cluster-3", instance: "3", err: false},
		},
	}
	for _, c := range cases {
		cluster, instance, err := removeHALabels(&c.labelsIn)
		assert.Equal(t, c.expected.cluster, cluster)
		assert.Equal(t, c.expected.instance, instance)
		if err != nil && c.expected.err == false {
			t.Fatalf("unexpected error: %s", err)
		}

	}
}
