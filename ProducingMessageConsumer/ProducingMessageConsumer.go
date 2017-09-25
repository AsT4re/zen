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
	msgHandler MessageHandler
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
	                               msgHandler  MessageHandler) (*ProducingMessageConsumer, error) {
	config := cluster.NewConfig()
	// Set config for consumer
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	// Set config for producer
	config.Producer.Return.Successes = true

	pmc := &ProducingMessageConsumer {
		prodTopic: prodTopic,
		msgHandler: msgHandler,
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
    return nil, errors.Wrap(err, "Create producer failed")
	}

	return pmc, nil
}

func (pmc *ProducingMessageConsumer) Consume() {
	inAcksHandler := newInputsAcksHandler(pmc.consumer)

	var wgOutAcksHandler sync.WaitGroup
	wgOutAcksHandler.Add(1)
	go handleOutputsAcks(&wgOutAcksHandler, pmc.producer, inAcksHandler)

	var wgMgsHandlers sync.WaitGroup
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
ConsumerLoop:
	for {
    select {
    case msg := <-pmc.consumer.Messages():
			pmc.inAcksHand.firstOffsetInit(msg.Topic, msg.Partition, msg.Offset)
			log.Printf("Handle message offset %d\n", msg.Offset)
			wgMgsHandlers.Add(1)
			go func(msg *sarama.ConsumerMessage) {
				defer wgMgsHandlers.Done()
				outputs, err := pmc.msgHandler.Process(msg.Value)
				if err == nil {
					// Produce outputs
					nbOutputs := len(outputs)
					if nbOutputs == 0 {
						inAcksHandler.ack(msg.Topic, msg.Partition, msg.Offset)
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
				} else {
					// Process failed, producing input on retry topic
				}
			}(msg)
    case <-signals:
			break ConsumerLoop
    }
	}

	log.Printf("\nFinish processing already handled messages...\n")
	wgMgsHandlers.Wait()
	pmc.producer.AsyncClose()
	wgOutAcksHandler.Wait()

	if err := pmc.consumer.Close(); err != nil {
		log.Println(err)
	}

	inAcksHandler.printNbMarkedOffsets()
}

func handleOutputsAcks(wg            *sync.WaitGroup,
                       producer      sarama.AsyncProducer,
	                     inAcksHandler *inputsAcksHandler) {
	defer wg.Done()
	offRem := make(map[string]map[int32]map[int64]int)
	successesChan := producer.Successes()
	errChan := producer.Errors()
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
				inAcksHandler.ack(inMetas.Topic, inMetas.Partition, inMetas.Offset)
				delete(o, inMetas.Offset)
			} else {
				o[inMetas.Offset] = v - 1
			}
		}
	}
}
