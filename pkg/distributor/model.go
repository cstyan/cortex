package distributor

import (
	// "sort"
	// "time"

	"github.com/golang/protobuf/proto"
)

// ProtoDescFactory makes new InstanceDescs
func ProtoInstanceDescFactory() proto.Message {
	return NewInstanceDesc()
}

// NewDesc returns an empty ring.Desc
func NewInstanceDesc() *InstanceDesc {
	return &InstanceDesc{
		// instance:  instance,
		// timestamp: timestamp,
	}
}
