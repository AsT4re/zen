package locationsproducer

import(
	"astare/zen/libs/objects"
	"astare/zen/libs/randcoords"
	"github.com/golang/protobuf/proto"
	srma   "github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"log"
	"math/rand"
	"sync"
	"time"
)

func ProduceRandomLocations(topic string, nbMsgs int, seed int64) error {
	if topic == "" {
		return errors.New("missing mandatory flag 'topic'")
	}

	config := srma.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := srma.NewAsyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
    return errors.Wrap(err, "Fail to init producer")
	}

	var (
    wg                sync.WaitGroup
    successes, errs int
	)

	wg.Add(1)
	go func() {
    defer wg.Done()
    for range producer.Successes() {
			successes++
    }
	}()

	wg.Add(1)
	go func() {
    defer wg.Done()
    for err := range producer.Errors() {
			log.Println(err)
			errs++
    }
	}()

	if seed == 0 {
		seed = time.Now().UnixNano()
	}

	r := rand.New(rand.NewSource(seed))

	for i := 0; i < nbMsgs; i++ {
    message, err := newUserPosMessage(topic, r)
		if err != nil {
			return err
		}

		producer.Input() <- message
	}

	producer.AsyncClose()

	wg.Wait()

	if errs > 0 {
		return errors.New("Fail to produce some messages")
	}

	return nil
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
