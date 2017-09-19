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

type MsgAckSynchronizer struct {
	markable int64
	acked map[int64]bool
	consumer *cluster.Consumer
	mux sync.Mutex
}

// Mark a specific offset as acknowledged
// Only mark offset if all previous offsets have been acknowledged too
func (synchronizer *MsgAckSynchronizer) ack(off int64, partition int32) {
	synchronizer.mux.Lock()
	defer synchronizer.mux.Unlock()
	if off == synchronizer.markable {
		next := off + 1
		for {
			_, ok := synchronizer.acked[next]
			if !ok {
				break
			}
			delete(synchronizer.acked, next)
			next++
		}
		synchronizer.markable = next
		synchronizer.consumer.MarkPartitionOffset(*topicUserLoc, partition, next-1, "")
	} else {
		synchronizer.acked[off] = true
	}
}

func (synchronizer *MsgAckSynchronizer) lastMarkedOffset() (int64, error) {
	if synchronizer.markable == 0 {
		return 0, fmt.Errorf("Any offset has been marked yet for this partition")
	}
	return synchronizer.markable - 1, nil
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
	consumerConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	brokers := []string{"127.0.0.1:9092"}
	topics := []string{*topicUserLoc}
	consumer, err := cluster.NewConsumer(brokers, "fencepub-group", topics, consumerConfig)
	if err != nil {
		log.Panicln(err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Consume first message in order to initialize synchronize
	var msg *sarama.ConsumerMessage
	select {
	case msg = <-consumer.Messages():
	case <-signals:
	}

	if msg == nil {
		if err := consumer.Close(); err != nil {
				log.Println(err)
		}
		fmt.Println("Successfully processed messages: 0")
		return
	}

	synchronizer := &MsgAckSynchronizer {
		markable: msg.Offset,
		acked: make(map[int64]bool),
		consumer: consumer,
	}

	defer func(first int64) {
		if err := consumer.Close(); err != nil {
			log.Println(err)
		}

		var nbConsumed int64
		if last, err := synchronizer.lastMarkedOffset(); err == nil {
			nbConsumed = last - first + 1
		}

		fmt.Printf("Successfully processed messages: %d\n", nbConsumed)
	}(msg.Offset)

		// Create producer
	producerConfig := sarama.NewConfig()
	producerConfig.Producer.Return.Successes = true
	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, producerConfig)
	if err != nil {
    log.Panicln(err)
	}

	var wgMsgHandle sync.WaitGroup
	log.Printf("Handle message offset %d\n", msg.Offset)
	wgMsgHandle.Add(1)
	go handleMessage(msg, dgCl, &wgMsgHandle, consumer, producer, synchronizer)

	var wgProducerAckChans sync.WaitGroup
	wgProducerAckChans.Add(1)
	go handleProducerAcks(&wgProducerAckChans, consumer, producer, synchronizer)

ConsumerLoop:
	for {
    select {
    case msg := <-consumer.Messages():
			log.Printf("Handle message offset %d\n", msg.Offset)
			wgMsgHandle.Add(1)
			go handleMessage(msg, dgCl, &wgMsgHandle, consumer, producer, synchronizer)
    case <-signals:
			break ConsumerLoop
    }
	}

	fmt.Printf("\nFinish processing already handled messages...\n")
	wgMsgHandle.Wait()
	producer.AsyncClose()
	wgProducerAckChans.Wait()
}

func handleAck(offRem map[int64]int, consumerInfos *ConsumerMsgInfos, synchronizer *MsgAckSynchronizer) {
	v, ok := offRem[consumerInfos.Offset]
	if !ok {
		offRem[consumerInfos.Offset] = consumerInfos.NbFences
		v = consumerInfos.NbFences
	}

	if v == 1 {
		synchronizer.ack(consumerInfos.Offset, consumerInfos.Partition)
		delete(offRem, consumerInfos.Offset)
	} else {
		offRem[consumerInfos.Offset] = v - 1
	}
}

func handleProducerAcks(wg *sync.WaitGroup, consumer *cluster.Consumer, producer sarama.AsyncProducer, synchronizer *MsgAckSynchronizer) {
	defer wg.Done()
	offRem := make(map[int64]int)
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
			handleAck(offRem, consumerInfos, synchronizer)
		case err, ok := <-errChan:
			if !ok {
				errChan = nil
				break
			}
			consumerInfos := err.Msg.Metadata.(*ConsumerMsgInfos)
			handleAck(offRem, consumerInfos, synchronizer)
		}

		if successesChan == nil && errChan == nil {
			break
		}
	}
}

func handleMessage(msg *sarama.ConsumerMessage,
	                 dgCl *dgclient.DGClient,
	                 wg *sync.WaitGroup,
	                 consumer *cluster.Consumer,
	                 producer sarama.AsyncProducer,
	                 synchronizer *MsgAckSynchronizer) {
	defer wg.Done()
	userPos := &objects.UserLocation {}
	if err := proto.Unmarshal(msg.Value, userPos); err != nil {
		log.Println(err)
		return
	}

	fences, err := dgCl.GetFencesContainingPos(userPos.Long, userPos.Lat)
	if err != nil {
		log.Println(err)
		return
	}

	nbFences := len(fences.Root)

	if nbFences == 0 {
		fmt.Printf("No fences, ack message: %d\n", msg.Offset)
		synchronizer.ack(msg.Offset, msg.Partition)
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
