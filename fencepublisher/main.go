package main

import(
	"astare/zen/objects"
	"astare/zen/dgclient"
	"flag"
	srma "github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"os"
	"os/signal"
	"log"
)

var (
	topicUserLoc = flag.String("topic-user-loc", "", "Topic where to consume UserLocation messages")
	topicUserFence = flag.String("topic-user-fence", "", "Topic where to produce UserFence messages")
	dgNbConns = flag.Uint("dg-conns-pool", 10, "Number of connections to DGraph")
	dgHost = flag.String("dg-host-and-port", "127.0.0.1:9080", "Dgraph database hostname and port")
)

func main() {
	flag.Parse()

	if *topicUserLoc == "" {
		log.Fatalln("missing mandatory flag 'topic-user-loc'")
	}

	if *topicUserFence == "" {
		log.Fatalln("missing mandatory flag 'topic-user-fence'")
	}

	consumer, err := srma.NewConsumer([]string{"localhost:9092"}, nil)
	if err != nil {
    log.Fatalln(err)
	}

	defer func() {
    if err := consumer.Close(); err != nil {
			log.Fatalln(err)
    }
	}()

	partitionConsumer, err := consumer.ConsumePartition(*topicUserLoc, 0, srma.OffsetOldest)
	if err != nil {
    log.Panicln(err)
	}

	defer func() {
    if err := partitionConsumer.Close(); err != nil {
			log.Fatalln(err)
    }
	}()

	// Create producer
	producer, err := srma.NewAsyncProducer([]string{"localhost:9092"}, nil)
	if err != nil {
    log.Panicln(err)
	}

	go func() {
    for err := range producer.Errors() {
			log.Println(err)
    }
	}()

	defer producer.AsyncClose()

	// Init connection to DGraph
	dgCl, err := dgclient.NewDGClient(*dgHost, *dgNbConns)
	if err != nil {
		log.Panicln(err)
	}
	if err := dgCl.Init(); err != nil {
		log.Panicln(err)
	}
	defer dgCl.Close()

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	consumed := 0
ConsumerLoop:
	for {
    select {
    case msg := <-partitionConsumer.Messages():
			log.Printf("Consumed message offset %d\n", msg.Offset)
			if err := handleMessage(producer, dgCl, msg.Value); err != nil {
				log.Println("Error handling consumed message: ", err)
			}
			consumed++

    case <-signals:
			break ConsumerLoop
    }
	}

	log.Printf("Consumed: %d\n", consumed)
}

func handleMessage(producer srma.AsyncProducer, dgCl *dgclient.DGClient, msg []byte) error {
	userPos := &objects.UserLocation {}
	if err := proto.Unmarshal(msg, userPos); err != nil {
		return err
	}

	fences, err := dgCl.GetFencesContainingPos(userPos.Long, userPos.Lat)
	log.Printf("user pos long: %f, user pos lat: %f\n", userPos.Long, userPos.Lat)
	if err != nil {
		return err
	}

	for _, fence := range fences.Root {
		newFence := &objects.UserFence {
			UserId: userPos.GetUserId(),
			PlaceName: fence.Name,
		}

		log.Printf("user id: %d, place name: %s\n", newFence.UserId, newFence.PlaceName)

		data, err := proto.Marshal(newFence)
		if err != nil {
			return err
		}

		producer.Input() <- &srma.ProducerMessage{Topic: *topicUserFence, Value: srma.ByteEncoder(data)}
	}

	return nil
}
