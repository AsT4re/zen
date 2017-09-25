package pmc

import (
	"sync"
	"log"
	cluster "github.com/bsm/sarama-cluster"
)

type acksSynchronizer struct {
	first    int64
	markable int64
	acked    map[int64]bool
}

// Mark a specific offset as acknowledged
// Only mark offset if all previous offsets have been acknowledged too
func (syncer *acksSynchronizer) ack(cons *cluster.Consumer, topic string, partition int32, off int64) {
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
		cons.MarkPartitionOffset(topic, partition, next-1, "")
	} else {
		syncer.acked[off] = true
	}
}

func (syncer *acksSynchronizer) nbMarkedOffsets() int64 {
	return syncer.markable - syncer.first
}

type inputsAcksHandler struct {
	m    map[string]map[int32]*acksSynchronizer
	cons *cluster.Consumer
	mux  sync.Mutex
}

func newInputsAcksHandler(cons *cluster.Consumer) *inputsAcksHandler {
	return &inputsAcksHandler {
		m:    make(map[string]map[int32]*acksSynchronizer),
		cons: cons,
	}
}

func (h *inputsAcksHandler) firstOffsetInit(topic string, partition int32, off int64) {
	h.mux.Lock()
	defer h.mux.Unlock()

	p, ok := h.m[topic]
	if !ok {
		p = make(map[int32]*acksSynchronizer)
		h.m[topic] = p
	}

	var syncer *acksSynchronizer
	syncer, ok = p[partition]
	if !ok {
		syncer = &acksSynchronizer {
			first: off,
			markable: off,
			acked: make(map[int64]bool),
		}
		p[partition] = syncer
	}
}

func (h *inputsAcksHandler) ack(topic string, partition int32, off int64) {
	h.mux.Lock()
	defer h.mux.Unlock()

	h.m[topic][partition].ack(h.cons, topic, partition, off)
}

//If called after consumer.close() and no errors have been notified
//due to kafka commit request issue, the marked offsets are actually
//the committed offsets if commit request succeeded on kafka
func (h *inputsAcksHandler) printNbMarkedOffsets() {
	h.mux.Lock()
	defer h.mux.Unlock()
	for topicName, parts := range h.m {
		nb := int64(0)
		for _, syncer := range parts {
			nb += syncer.nbMarkedOffsets()
		}
		log.Printf("Number of processed messages for topic %s: %d\n", topicName, nb)
	}
}

