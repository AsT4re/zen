package main

import(
	"astare/zen/objects"
	"astare/zen/randcoords"
	"flag"
	"github.com/golang/protobuf/proto"
	srma "github.com/Shopify/sarama"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
)

var (
	seed = flag.Int64("seed", 0, "Seed for generation of random positions coordinates, use same seed for same sequence")
	nbMsgs = flag.Uint("nb-msgs", 10, "Number of position messages to produce")
	topic = flag.String("topic", "", "Topic where to produce messages")
)

func main() {
	flag.Parse()

	if *topic == "" {
		log.Fatal("missing mandatory flag 'topic'")
	}

	config := srma.NewConfig()
	config.Producer.Return.Successes = true

	producer, err := srma.NewAsyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
    panic(err)
	}

	// Trap SIGINT to trigger a graceful shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var (
    wg                          sync.WaitGroup
    enqueued, successes, errors int
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
			errors++
    }
	}()

	r := rand.New(rand.NewSource(*seed))

ProducerLoop:
	for i := uint(0); i < *nbMsgs; i++ {
    message, err := newUserPosMessage(r)
		if err != nil {
			log.Fatal("error creating UserPos message: ", err)
			continue ProducerLoop
		}
    select {
    case producer.Input() <- message:
			enqueued++

    case <-signals:
			producer.AsyncClose() // Trigger a shutdown of the producer.
			break ProducerLoop
    }
	}

	wg.Wait()

	log.Printf("Successfully produced: %d; errors: %d\n", successes, errors)
}

func newUserPosMessage(r *rand.Rand) (*srma.ProducerMessage, error) {
	userPos := &objects.UserLocation {
		UserId: r.Uint32(),
		Lat: randcoords.GetRandCoord(r, -85, 85),
		Long: randcoords.GetRandCoord(r, -180, 180),
	}

	data, err := proto.Marshal(userPos)
	if err != nil {
		return nil, err
	}

	return &srma.ProducerMessage{Topic: *topic, Value: srma.ByteEncoder(data)}, nil
}
