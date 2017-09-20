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
	Topic     string
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
	consumerConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	brokers := []string{"127.0.0.1:9092"}
	topics := []string{*topicUserLoc}
	consumer, err := cluster.NewConsumer(brokers, "fencepub-group", topics, consumerConfig)
	if err != nil {
		log.Panicln(err)
	}

	// Create producer
	producerConfig := sarama.NewConfig()
	producerConfig.Producer.Return.Successes = true
	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, producerConfig)
	if err != nil {
    log.Panicln(err)
	}

	syncers := &MsgAckSyncronizersMap{
		t: make(map[string]*MsgAckSyncronizer),
	}

	var wgProducerAckChans sync.WaitGroup
	wgProducerAckChans.Add(1)
	go handleProducerAcks(&wgProducerAckChans, consumer, producer, syncers)

	var wgMsgHandle sync.WaitGroup
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

ConsumerLoop:
	for {
    select {
    case msg := <-consumer.Messages():
			log.Printf("Handle message offset %d\n", msg.Offset)
			syncer, ok := syncers.t[msg.Topic]
			if !ok {
				fmt.Printf("topic: %s\n", msg.Topic)
				// Create new message ack syncronizer on first message for a topic
				syncers.t[msg.Topic] = &MsgAckSyncronizer {
					markable: msg.Offset,
					acked: make(map[int64]bool),
					consumer: consumer,
				}
			}
			wgMsgHandle.Add(1)
			go handleMessage(msg, dgCl, &wgMsgHandle, consumer, producer, syncer)
    case <-signals:
			break ConsumerLoop
    }
	}

	fmt.Printf("\nFinish processing already handled messages...\n")
	wgMsgHandle.Wait()
	producer.AsyncClose()
	wgProducerAckChans.Wait()

	if err := consumer.Close(); err != nil {
		log.Println(err)
	}

	syncers.PrintNbMarkedOffsets()
}

func handleAck(offRem map[int64]int, consumerInfos *ConsumerMsgInfos, syncer *MsgAckSyncronizer) {
	v, ok := offRem[consumerInfos.Offset]
	if !ok {
		offRem[consumerInfos.Offset] = consumerInfos.NbFences
		v = consumerInfos.NbFences
	}

	if v == 1 {
		syncer.Ack(consumerInfos.Offset, consumerInfos.Partition)
		delete(offRem, consumerInfos.Offset)
	} else {
		offRem[consumerInfos.Offset] = v - 1
	}
}

func handleProducerAcks(wg *sync.WaitGroup, consumer *cluster.Consumer, producer sarama.AsyncProducer, syncers *MsgAckSyncronizersMap) {
	defer wg.Done()
	offRem := make(map[string]map[int64]int)
	successesChan := producer.Successes()
	errChan := producer.Errors()
	for {
		select {
		case msg, ok := <-successesChan:
			if !ok {
				successesChan = nil
				break
			}
			infos := msg.Metadata.(*ConsumerMsgInfos)
			_, okMap := offRem[infos.Topic]
			if !okMap {
				offRem[infos.Topic] = make(map[int64]int)
			}
			handleAck(offRem[infos.Topic], infos, syncers.t[infos.Topic])
		case err, ok := <-errChan:
			if !ok {
				errChan = nil
				break
			}
			infos := err.Msg.Metadata.(*ConsumerMsgInfos)
			_, okMap := offRem[infos.Topic]
			if !okMap {
				offRem[infos.Topic] = make(map[int64]int)
			}
			handleAck(offRem[infos.Topic], infos, syncers.t[infos.Topic])
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
	                 syncer *MsgAckSyncronizer) {
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
		syncer.Ack(msg.Offset, msg.Partition)
		return
	}

	metadata := &ConsumerMsgInfos{
		Offset: msg.Offset,
		Partition: msg.Partition,
		Topic: msg.Topic,
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
