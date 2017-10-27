package pmc

import(
	sarama   "github.com/Shopify/sarama"
	cluster  "github.com/bsm/sarama-cluster"
	errors   "github.com/pkg/errors"
	rec      "astare/zen/libs/datasrecorder"
	"os"
	"os/signal"
	"log"
	"fmt"
	"sync"
)

type ProducingMessageConsumer struct {
	consumer   *cluster.Consumer
	producer   sarama.AsyncProducer
	prodTopic  string
	consTopic  string
	msgHand    MessageHandler
	wgOutAcks  sync.WaitGroup
	dr         *rec.DatasRecorder
	deadLimit  int
	nbDead     int
	nbDeadMut  sync.Mutex
}

type inputMetadatas struct {
	Offset    int64
	Partition int32
	Topic     string
	NbOutputs int
}

func NewProducingMessageConsumer(brokers     []string,
	                               consGroupId string,
	                               consTopic   string,
	                               prodTopic   string,
	                               msgHand     MessageHandler,
	                               deadLimit   int,
                                 dr          *rec.DatasRecorder) (*ProducingMessageConsumer, error) {
	config := cluster.NewConfig()
	config.Group.Mode = cluster.ConsumerModePartitions
	// Set config for consumer
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	// Set config for producer
	config.Producer.Return.Successes = true

	pmc := &ProducingMessageConsumer {
		deadLimit: deadLimit,
		prodTopic: prodTopic,
		consTopic: consTopic,
		msgHand: msgHand,
		dr: dr,
	}

	if dr == nil {
		pmc.dr = rec.NewDatasRecorder(false)
	}

	pmc.dr.NewCollection("fences")
	pmc.dr.NewCollection("latencies")

	var err error
	// Create cluster of consumer for handling messages
	pmc.consumer, err = cluster.NewConsumer(brokers, consGroupId, []string{consTopic}, config)
	if err != nil {
		return nil, errors.Wrap(err, "Create cluster of consumers failed")
	}

	// Create producer for messages to be published
	pmc.producer, err = sarama.NewAsyncProducer(brokers, &config.Config)
	if err != nil {
		if errCons := pmc.consumer.Close(); errCons != nil {
			log.Printf("Fail to close consumer: %v\n", errCons)
		}
    return nil, errors.Wrap(err, "Create producer failed")
	}

	pmc.wgOutAcks.Add(1)
	go pmc.handleOutputsAcks()

	return pmc, nil
}

func (pmc *ProducingMessageConsumer) ConsumeLtd(limits map[string]map[int32]uint, total int) error {
	var wg sync.WaitGroup
	totalCount := 0
PartLoop:
	for {
		select {
		case part, ok := <-pmc.consumer.Partitions():
			if !ok {
				return errors.New("Channel has been closed prematurely")
			}

			p, ok := limits[part.Topic()]
			if ok {
				l, ok := p[part.Partition()]
				if ok {
					delete(p, part.Partition())
					if len(p) == 0 {
						delete(limits, part.Topic())
					}
					wg.Add(1)
					go func(pc cluster.PartitionConsumer, limit uint) {
						defer wg.Done()
						count := uint(0)
						for msg := range pc.Messages() {
							pmc.consumeMessage(msg)
							count++
							if count == limit {
								break
							}
						}
					}(part, l)
				}
			}

			totalCount++
			if  totalCount == total {
				break PartLoop
			}
		}
	}

	wg.Wait()

	return nil
}

func (pmc *ProducingMessageConsumer) Consume() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
PartLoop:
	for {
		select {
		case part, ok := <-pmc.consumer.Partitions():
			if !ok {
				return
			}
			go func(pc cluster.PartitionConsumer) {
				for msg := range pc.Messages() {
					pmc.consumeMessage(msg)
				}
			}(part)
		case <-signals:
			break PartLoop
		}
	}
}


func (pmc *ProducingMessageConsumer) Close() {
	pmc.producer.AsyncClose()
	pmc.wgOutAcks.Wait()

	if err := pmc.consumer.Close(); err != nil {
		log.Println(err)
	}
}

func (pmc *ProducingMessageConsumer) publishDeadLetter(msg *sarama.ConsumerMessage, messVal interface{}, procErr error) {
	outEncoded, err := pmc.msgHand.Marshal(&Message{
		Value: messVal,
		Metas: &MessageMetadatas{
			Error: fmt.Sprintf("%v", procErr),
		},
	})

	if err != nil {
		log.Panicln(err)
	}

	pmc.producer.Input() <- &sarama.ProducerMessage{
		Topic: pmc.consTopic + "-dead",
		Value: sarama.ByteEncoder(outEncoded),
		Metadata: &inputMetadatas{
			Offset: msg.Offset,
			Partition: msg.Partition,
			Topic: msg.Topic,
			NbOutputs: 1,
		},
	}
}

func (pmc *ProducingMessageConsumer) consumeMessage(msg *sarama.ConsumerMessage) {

	before := pmc.dr.Now()

	handMsg, err := pmc.msgHand.Unmarshal(msg.Value)
	if err != nil {
		log.Panicf("Error when unmarshalling message: %v", err)
	}

	prodMetas := &inputMetadatas{
		Offset: msg.Offset,
		Partition: msg.Partition,
		Topic: msg.Topic,
	}
	outputs, err := pmc.msgHand.Process(handMsg.Value)

	if err != nil {
		if pmc.deadLimit >= 0 {
			pmc.nbDeadMut.Lock()
			if pmc.nbDead == pmc.deadLimit {
				log.Panicf("Limit of dead letter messages reached (Err: %v)\n", err)
			}
			pmc.nbDead++
			pmc.nbDeadMut.Unlock()
		}
		log.Printf("Processing message failed, publishing in dead-letter queue (Err: %v)\n", err)
		pmc.publishDeadLetter(msg, handMsg.Value, err)
	} else {
		// Produce outputs
		nbOutputs := len(outputs)
		pmc.dr.AddToCollection("fences", float64(nbOutputs))

		if nbOutputs == 0 {
			pmc.consumer.MarkOffset(msg, "")
		} else {
			prodMetas.NbOutputs = nbOutputs
			for _, out := range outputs {
				pmc.producer.Input() <- &sarama.ProducerMessage{
					Topic: pmc.prodTopic,
					Value: sarama.ByteEncoder(out),
					Metadata: prodMetas,
				}
			}
		}
	}

	lat := pmc.dr.SinceTime(before)
	pmc.dr.AddToCollection("latencies", lat.Seconds()*1000)
}


func (pmc *ProducingMessageConsumer) handleOutputsAcks() {
	defer pmc.wgOutAcks.Done()
	offRem := make(map[string]map[int32]map[int64]int)
	successesChan := pmc.producer.Successes()
	errChan := pmc.producer.Errors()
	for {
		var ok bool
		var msg *sarama.ProducerMessage
		var err *sarama.ProducerError
		var inMetas *inputMetadatas
		select {
		case msg, ok = <-successesChan:
			if !ok {
				successesChan = nil
				break
			}
			inMetas = msg.Metadata.(*inputMetadatas)
			p, ok := offRem[inMetas.Topic]
			if !ok {
				p = make(map[int32]map[int64]int)
				offRem[inMetas.Topic] = p
			}
			o, ok := p[inMetas.Partition]
			if !ok {
				o = make(map[int64]int)
				p[inMetas.Partition] = o
			}

			v, ok := o[inMetas.Offset]
			if !ok {
				o[inMetas.Offset] = inMetas.NbOutputs
				v = inMetas.NbOutputs
			}

			if v == 1 {
				pmc.consumer.MarkPartitionOffset(inMetas.Topic, inMetas.Partition, inMetas.Offset, "")
				delete(o, inMetas.Offset)
			} else {
				o[inMetas.Offset] = v - 1
			}
		case err, ok = <-errChan:
			if !ok {
				errChan = nil
				break
			}
			log.Panicf("Publishing message (t: %s, p: %d, o: %d) error: %v\n",
				          err.Msg.Topic, err.Msg.Partition, err.Msg.Offset, err)
		}

		if successesChan == nil && errChan == nil {
			break
		}
	}
}
