package locationsproducer

import(
	"astare/zen/libs/objects"
	"astare/zen/libs/randcoords"
	"github.com/golang/protobuf/proto"
	srma   "github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"math/rand"
	"sync"
	"time"
)

type roundRobinPartitionerLtd struct {
	partition int32
	limit     int32
}

func roundRobinPartitionerLtdCtor(limit int32) srma.PartitionerConstructor {
	return func(topic string) srma.Partitioner {
		return &roundRobinPartitionerLtd{
			limit: limit,
		}
	}
}

func (p *roundRobinPartitionerLtd) Partition(message *srma.ProducerMessage, numPartitions int32) (int32, error) {
	if p.limit > numPartitions {
		return 0, errors.Errorf("Error: limit(%d) > numPartitions(%d) !", p.limit, numPartitions)
	}

	if p.partition >= p.limit {
		p.partition = 0
	}
	ret := p.partition
	p.partition++
	return ret, nil
}

func (p *roundRobinPartitionerLtd) RequiresConsistency() bool {
	return false
}

func ProduceRandomLocations(topic string, nbMsgs int, seed int64, parallel int32) (map[int32]uint, error) {
	if topic == "" {
		return nil, errors.New("missing mandatory flag 'topic'")
	}

	config := srma.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = roundRobinPartitionerLtdCtor(parallel)
	producer, err := srma.NewAsyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
    return nil, errors.Wrap(err, "Fail to init producer")
	}

	var (
    wg              sync.WaitGroup
	)

	min := int(parallel)
	if nbMsgs < min {
		min = nbMsgs
	}

	summary := make(map[int32]uint)
	for i := 0; i < min; i++ {
		summary[int32(i)] = 0
	}

	wg.Add(1)
	go func() {
    defer wg.Done()
    for msg := range producer.Successes() {
			summary[msg.Partition]++
    }
	}()

	var lastError error
	wg.Add(1)
	go func() {
    defer wg.Done()
    for err := range producer.Errors() {
			lastError = errors.Wrap(err, "Producer last error")
    }
	}()

	if seed == 0 {
		seed = time.Now().UnixNano()
	}

	r := rand.New(rand.NewSource(seed))

	for i := 0; i < nbMsgs; i++ {
    message, err := newUserPosMessage(topic, r)
		if err != nil {
			return nil, err
		}

		producer.Input() <- message
	}

	producer.AsyncClose()

	wg.Wait()

	return summary, lastError
}

func newUserPosMessage(topic string, r *rand.Rand) (*srma.ProducerMessage, error) {
	userPos := &objects.UserLocation {
		Value: &objects.UserLocationValue {
			UserId: r.Uint32(),
			Lat: randcoords.GetRandCoord(r, -85, 85),
			Long: randcoords.GetRandCoord(r, -180, 180),
		},
	}

	data, err := proto.Marshal(userPos)
	if err != nil {
		return nil, errors.Wrap(err, "Fail to Marshal location")
	}

	return &srma.ProducerMessage{Topic: topic, Value: srma.ByteEncoder(data)}, nil
}
