package main

import (
	"sync"
	cluster "github.com/bsm/sarama-cluster"
	"fmt"
)

type MsgAckSyncronizer struct {
	first    int64
	markable int64
	acked    map[int64]bool
	consumer *cluster.Consumer
	mux      sync.Mutex
}

// Mark a specific offset as acknowledged
// Only mark offset if all previous offsets have been acknowledged too
func (syncer *MsgAckSyncronizer) Ack(off int64, partition int32) {
	syncer.mux.Lock()
	defer syncer.mux.Unlock()
	if off == syncer.markable {
		next := off + 1
		for {
			_, ok := syncer.acked[next]
			if !ok {
				break
			}
			delete(syncer.acked, next)
			next++
		}
		syncer.markable = next
		syncer.consumer.MarkPartitionOffset(*topicUserLoc, partition, next-1, "")
	} else {
		syncer.acked[off] = true
	}
}

func (syncer *MsgAckSyncronizer) NbMarkedOffsets() int64 {
	return syncer.markable - syncer.first
}

type MsgAckSyncronizersMap struct {
	t map[string]*MsgAckSyncronizer
}

// Execute this method after consumer.close() for better estimation
func (syncers *MsgAckSyncronizersMap) PrintNbMarkedOffsets() {
	nb := int64(0)
	for _, v := range syncers.t {
		nb += v.NbMarkedOffsets()
	}

	fmt.Printf("Successfully processed messages: %d\n", nb)
}

