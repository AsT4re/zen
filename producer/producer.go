package main

import(
	"github.com/Shopify/sarama"
	"os"
	"os/signal"
	"sync"
	"log"
	"zen/protobuf/proto"
)

//import proto1 "github.com/golang/protobuf/proto"

func main() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, config)
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

	userPos := proto.UserLocation {
		UserId: 1,
		Lat: 30.12
		Long: 35.123
	}

ProducerLoop:
	for {
    message := &sarama.ProducerMessage{Topic: "my_topic", Value: sarama.StringEncoder("testing 123")}
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
