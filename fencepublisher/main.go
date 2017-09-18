package main

import(
	"astare/zen/objects"
	"astare/zen/dgclient"
	"flag"
	sarama "github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/golang/protobuf/proto"
	"os"
	"os/signal"
	"log"
	"sync"
	"fmt"
)

var (
	topicUserLoc = flag.String("topic-user-loc", "", "Topic where to consume UserLocation messages")
	topicUserFence = flag.String("topic-user-fence", "", "Topic where to produce UserFence messages")
	dgNbConns = flag.Uint("dg-conns-pool", 10, "Number of connections to DGraph")
	dgHost = flag.String("dg-host-and-port", "127.0.0.1:9080", "Dgraph database hostname and port")
)

type ConsumerMsgInfos struct {
	Offset    int64
	Partition int32
	NbFences  int
}

func main() {
	flag.Parse()

	if *topicUserLoc == "" {
		log.Fatalln("missing mandatory flag 'topic-user-loc'")
	}

	if *topicUserFence == "" {
		log.Fatalln("missing mandatory flag 'topic-user-fence'")
	}

	// Create client + init connection
	dgCl, err := dgclient.NewDGClient(*dgHost, *dgNbConns)
	if err != nil {
		log.Panicln(err)
	}
	if err := dgCl.Init(); err != nil {
		log.Panicln(err)
	}
	defer dgCl.Close()

	// Create consumer
	consumerConfig := cluster.NewConfig()
	consumerConfig.Consumer.Return.Errors = true
	brokers := []string{"127.0.0.1:9092"}
	topics := []string{*topicUserLoc}
	consumer, err := cluster.NewConsumer(brokers, "fencepub-group", topics, consumerConfig)
	if err != nil {
		log.Panicln(err)
	}

	defer func() {
    if err := consumer.Close(); err != nil {
			log.Println(err)
    }
	}()

	// Create producer
	producerConfig := sarama.NewConfig()
	producerConfig.Producer.Return.Successes = true
	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, producerConfig)
	if err != nil {
    log.Panicln(err)
	}

	var wgProducerAckChans sync.WaitGroup
	wgProducerAckChans.Add(1)
	go handleProducerAcks(&wgProducerAckChans, producer, consumer)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var wgMsgHandle sync.WaitGroup
	consumed := 0
ConsumerLoop:
	for {
    select {
    case msg := <-consumer.Messages():
			log.Printf("Consumed message offset %d\n", msg.Offset)
			wgMsgHandle.Add(1)
			go handleMessage(consumer, producer, dgCl, msg, &wgMsgHandle)
			consumed++
    case <-signals:
			break ConsumerLoop
    }
	}

	fmt.Printf("\nFinish processing already handled messages...\n")
	wgMsgHandle.Wait()
	producer.AsyncClose()
	wgProducerAckChans.Wait()
	fmt.Printf("Consumed messages estimation: %d\n", consumed)
}

func handleAck(offRem map[int64]int, offErr map[int64]bool, consumerInfos *ConsumerMsgInfos, consumer *cluster.Consumer) {
	v, ok := offRem[consumerInfos.Offset]
	if !ok {
		offRem[consumerInfos.Offset] = consumerInfos.NbFences
		v = consumerInfos.NbFences
	}

	if v == 1 {
		if _, ok := offErr[consumerInfos.Offset]; !ok {
			consumer.MarkPartitionOffset(*topicUserLoc, consumerInfos.Partition, consumerInfos.Offset, "")
		} else {
			delete(offErr, consumerInfos.Offset)
		}

		delete(offRem, consumerInfos.Offset)
	} else {
		offRem[consumerInfos.Offset] = v - 1
	}
}

func handleProducerAcks(wg *sync.WaitGroup, producer sarama.AsyncProducer, consumer *cluster.Consumer) {
	defer wg.Done()
	offRem := make(map[int64]int)
	offErr := make(map[int64]bool)
	successesChan := producer.Successes()
	errChan := producer.Errors()
	for {
		select {
		case msg, ok := <-successesChan:
			if !ok {
				successesChan = nil
				break
			}
			consumerInfos := msg.Metadata.(*ConsumerMsgInfos)
			handleAck(offRem, offErr, consumerInfos, consumer)
		case err, ok := <-errChan:
			if !ok {
				errChan = nil
				break
			}
			consumerInfos := err.Msg.Metadata.(*ConsumerMsgInfos)
			offErr[consumerInfos.Offset] = true
			handleAck(offRem, offErr, consumerInfos, consumer)
		}

		if successesChan == nil && errChan == nil {
			break
		}
	}
}

func handleMessage(consumer *cluster.Consumer,
	                 producer sarama.AsyncProducer,
                   dgCl *dgclient.DGClient,
	                 msg *sarama.ConsumerMessage,
	                 wg *sync.WaitGroup) {
	defer wg.Done()
	userPos := &objects.UserLocation {}
	if err := proto.Unmarshal(msg.Value, userPos); err != nil {
		log.Println(err)
		return
	}

	fences, err := dgCl.GetFencesContainingPos(userPos.Long, userPos.Lat)
	log.Printf("user pos long: %f, user pos lat: %f\n", userPos.Long, userPos.Lat)
	if err != nil {
		log.Println(err)
		return
	}

	nbFences := len(fences.Root)

	if nbFences == 0 {
		consumer.MarkOffset(msg, "")
		return
	}

	metadata := &ConsumerMsgInfos{
		Offset: msg.Offset,
		Partition: msg.Partition,
		NbFences: nbFences,
	}

	for _, fence := range fences.Root {
		newFence := &objects.UserFence {
			UserId: userPos.GetUserId(),
			PlaceName: fence.Name,
		}

		data, err := proto.Marshal(newFence)
		if err != nil {
			log.Println(err)
			continue
		}

		producer.Input() <- &sarama.ProducerMessage{
			Topic: *topicUserFence,
			Value: sarama.ByteEncoder(data),
			Metadata: metadata,
		}
	}
}
