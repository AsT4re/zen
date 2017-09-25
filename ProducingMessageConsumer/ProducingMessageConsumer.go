package pmc

import(
	sarama   "github.com/Shopify/sarama"
	cluster  "github.com/bsm/sarama-cluster"
	errors   "github.com/pkg/errors"
	"os"
	"os/signal"
	"log"
	"sync"
)

type ProducingMessageConsumer struct {
	consumer   *cluster.Consumer
	producer   sarama.AsyncProducer
	prodTopic  string
	msgHand    MessageHandler
	inAcksHand *inputsAcksHandler
	wgOutAcks  sync.WaitGroup
	wgMsgHand  sync.WaitGroup
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
	                               msgHand     MessageHandler) (*ProducingMessageConsumer, error) {
	config := cluster.NewConfig()
	// Set config for consumer
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	// Set config for producer
	config.Producer.Return.Successes = true

	pmc := &ProducingMessageConsumer {
		prodTopic: prodTopic,
		msgHand: msgHand,
	}

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

	pmc.inAcksHand = newInputsAcksHandler(pmc.consumer)

	pmc.wgOutAcks.Add(1)
	go pmc.handleOutputsAcks()

	return pmc, nil
}

func (pmc *ProducingMessageConsumer) Consume() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
ConsumerLoop:
	for {
    select {
    case msg := <-pmc.consumer.Messages():
			pmc.inAcksHand.firstOffsetInit(msg.Topic, msg.Partition, msg.Offset)
			log.Printf("Handle message offset %d\n", msg.Offset)
			handMsg, err := pmc.msgHand.Unmarshal(msg.Value)
			if err != nil {
				// Todo: put in dead letter queue
				log.Println("Unable to unmarshal message")
				continue ConsumerLoop
			}
			pmc.wgMsgHand.Add(1)
			go func(msg *sarama.ConsumerMessage, handMsg *Message) {
				defer pmc.wgMsgHand.Done()
				outputs, err := pmc.msgHand.Process(handMsg.Value)
				if err != nil {
					// Todo: put in retry queue or dead letter if all retries done
					return
				}
				// Produce outputs
				nbOutputs := len(outputs)
				if nbOutputs == 0 {
					pmc.inAcksHand.ack(msg.Topic, msg.Partition, msg.Offset)
					return
				}
				for _, out := range outputs {
					metadatas := &inputMetadatas{
						Offset: msg.Offset,
						Partition: msg.Partition,
						Topic: msg.Topic,
						NbOutputs: nbOutputs,
					}
					pmc.producer.Input() <- &sarama.ProducerMessage{
						Topic: pmc.prodTopic,
						Value: sarama.ByteEncoder(out),
						Metadata: metadatas,
					}
				}
			}(msg, handMsg)
    case <-signals:
			break ConsumerLoop
    }
	}
}

func (pmc *ProducingMessageConsumer) Close() {
	log.Printf("\nFinish processing already handled messages...\n")
	pmc.wgMsgHand.Wait()
	pmc.producer.AsyncClose()
	pmc.wgOutAcks.Wait()

	if err := pmc.consumer.Close(); err != nil {
		log.Println(err)
	}

	pmc.inAcksHand.printNbMarkedOffsets()
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
		case err, ok = <-errChan:
			if !ok {
				errChan = nil
				break
			}
			inMetas = err.Msg.Metadata.(*inputMetadatas)
			log.Printf("Producing output error (Input:'%s'p:'%d'o:'%d')",
				         inMetas.Topic, inMetas.Partition, inMetas.Offset)
		}

		if successesChan == nil && errChan == nil {
			break
		}

		if ok {
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
				pmc.inAcksHand.ack(inMetas.Topic, inMetas.Partition, inMetas.Offset)
				delete(o, inMetas.Offset)
			} else {
				o[inMetas.Offset] = v - 1
			}
		}
	}
}
