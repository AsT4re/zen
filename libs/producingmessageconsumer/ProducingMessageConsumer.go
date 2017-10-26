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
	"time"
)

type ProducingMessageConsumer struct {
	consumer   *cluster.Consumer
	producer   sarama.AsyncProducer
	prodTopic  string
	consTopic  string
	msgHand    MessageHandler
	inAcksHand *inputsAcksHandler
	wgOutAcks  sync.WaitGroup
	wgMsgHand  sync.WaitGroup
	cond       *sync.Cond
	msgs       int
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
		cond: sync.NewCond(&sync.Mutex{}),
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

	pmc.inAcksHand = newInputsAcksHandler(pmc.consumer)

	pmc.wgOutAcks.Add(1)
	go pmc.handleOutputsAcks()

	return pmc, nil
}

func (pmc *ProducingMessageConsumer) Consume(nbtotal int, msgsLimit int) {
	var procmsgs int
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
ConsumerLoop:
	for {
    select {
    case msg := <-pmc.consumer.Messages():
			if msgsLimit > 0 {
				pmc.cond.L.Lock()
				if pmc.msgs == msgsLimit {
					pmc.cond.Wait()
				}
				pmc.msgs++
				pmc.cond.L.Unlock()
			}
			pmc.inAcksHand.firstOffsetInit(msg.Topic, msg.Partition, msg.Offset)
			before := pmc.dr.Now()
			handMsg, err := pmc.msgHand.Unmarshal(msg.Value)
			if err != nil {
				log.Panicf("Error when unmarshalling message: %v", err)
			}
			elapsed := pmc.dr.SinceTime(before)

			pmc.wgMsgHand.Add(1)
			go pmc.processMessage(msg, handMsg, elapsed, msgsLimit)
    case <-signals:
			break ConsumerLoop
    }

		if nbtotal > 0 {
			procmsgs++
			if procmsgs == nbtotal {
				break ConsumerLoop
			}
		}
	}
}

func (pmc *ProducingMessageConsumer) WaitProcessingMessages() {
	pmc.wgMsgHand.Wait()
	pmc.msgs = 0
}

func (pmc *ProducingMessageConsumer) Close() {
	pmc.WaitProcessingMessages()
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
			error: fmt.Sprintf("%v", procErr),
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

func (pmc *ProducingMessageConsumer) processMessage(msg *sarama.ConsumerMessage,
	                                                  handMsg *Message,
	                                                  elapsed time.Duration,
                                                    msgsLimit int) {
	defer func() {
		if msgsLimit > 0 {
			pmc.cond.L.Lock()
			pmc.msgs--
			pmc.cond.Signal()
			pmc.cond.L.Unlock()
		}
		pmc.wgMsgHand.Done()
	}()
	prodMetas := &inputMetadatas{
		Offset: msg.Offset,
		Partition: msg.Partition,
		Topic: msg.Topic,
	}
	before := pmc.dr.Now()
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
			pmc.inAcksHand.ack(msg.Topic, msg.Partition, msg.Offset)
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

	lat := pmc.dr.SinceTime(before) + elapsed
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
				pmc.inAcksHand.ack(inMetas.Topic, inMetas.Partition, inMetas.Offset)
				delete(o, inMetas.Offset)
			} else {
				o[inMetas.Offset] = v - 1
			}
		case err, ok = <-errChan:
			if !ok {
				errChan = nil
				break
			}
			// On sarama producer side, retries have been made, so there is a
			// a good chance that the issue comes from kafka and in that case
			// it's better to stop the program in order to avoid other errors
			log.Panicf("Publishing message (t: %s, p: %d, o: %d) error: %v\n",
				          err.Msg.Topic, err.Msg.Partition, err.Msg.Offset, err)
		}

		if successesChan == nil && errChan == nil {
			break
		}
	}
}
