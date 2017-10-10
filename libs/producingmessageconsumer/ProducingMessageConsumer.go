package pmc

import(
	sarama   "github.com/Shopify/sarama"
	cluster  "github.com/bsm/sarama-cluster"
	errors   "github.com/pkg/errors"
	rec      "astare/zen/libs/latencyrecorder"
	"os"
	"os/signal"
	"log"
	"sync"
	"time"
	"strconv"
)

type ProducingMessageConsumer struct {
	maxRetry   uint32
	consumer   *cluster.Consumer
	producer   sarama.AsyncProducer
	prodTopic  string
	consTopic  string
	msgHand    MessageHandler
	inAcksHand *inputsAcksHandler
	wgOutAcks  sync.WaitGroup
	wgMsgHand  sync.WaitGroup
	timers     map[string]map[int32]map[int64]*time.Timer
	timersMux  sync.Mutex
	cond       *sync.Cond
	msgs       int
	msgsLimit  int
	nbtotal    int
	procmsgs   int
	lr         *rec.LatencyRecorder
}

type inputMetadatas struct {
	Offset    int64
	Partition int32
	Topic     string
	NbOutputs int
}

const lenLatencies = 9

var latencies = [lenLatencies]time.Duration {
	time.Duration(2)*time.Second,
	time.Duration(2)*time.Second,
	time.Duration(10)*time.Second,
	time.Duration(30)*time.Second,
	time.Duration(1)*time.Minute,
	time.Duration(10)*time.Minute,
	time.Duration(1)*time.Hour,
	time.Duration(1)*time.Hour,
	time.Duration(24)*time.Hour,
}

var topicsByRetry [lenLatencies]string

func NewProducingMessageConsumer(brokers     []string,
	                               consGroupId string,
	                               consTopic   string,
	                               prodTopic   string,
	                               msgHand     MessageHandler,
	                               maxRetry    uint32,
	                               msgsLimit   int,
	                               nbtotal     int,
                                 lr          *rec.LatencyRecorder) (*ProducingMessageConsumer, error) {
	if int(maxRetry) >  lenLatencies {
		return nil, errors.Errorf("max retries must be <= %d", lenLatencies)
	}
	config := cluster.NewConfig()
	// Set config for consumer
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	// Set config for producer
	config.Producer.Return.Successes = true

	pmc := &ProducingMessageConsumer {
		maxRetry: maxRetry,
		prodTopic: prodTopic,
		consTopic: consTopic,
		msgHand: msgHand,
		timers: make(map[string]map[int32]map[int64]*time.Timer),
		msgsLimit: msgsLimit,
		cond: sync.NewCond(&sync.Mutex{}),
		nbtotal: nbtotal,
		lr: lr,
	}

	if lr == nil {
		pmc.lr = rec.NewLatencyRecorder(false)
	}

	pmc.lr.SetCollectionSpecs("Fences found:", "")

	var err error
	topics := append(getRetryTopicNames(consTopic), consTopic)
	// Create cluster of consumer for handling messages
	pmc.consumer, err = cluster.NewConsumer(brokers, consGroupId, topics, config)
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
			if pmc.msgsLimit > 0 {
				pmc.cond.L.Lock()
				if pmc.msgs == pmc.msgsLimit {
					pmc.cond.Wait()
				}
				pmc.msgs++
				pmc.cond.L.Unlock()
			}
			pmc.inAcksHand.firstOffsetInit(msg.Topic, msg.Partition, msg.Offset)
			before := pmc.lr.Now()
			handMsg, err := pmc.msgHand.Unmarshal(msg.Value)
			if err != nil {
				log.Printf("Publishing in dead letter: unable to Unmarshal message: %v\n", err)
				pmc.publishDeadLetter(msg)
				continue ConsumerLoop
			}
			elapsed := pmc.lr.SinceTime(before)

			if handMsg.Metas != nil {
				var sched time.Time
				if err := sched.UnmarshalText(handMsg.Metas.Schedule); err != nil {
					log.Printf("Publishing in dead letter: unable to Unmarshal date time: %v\n", err)
					pmc.publishDeadLetter(msg)
					continue ConsumerLoop
				}
				pmc.wgMsgHand.Add(1)
				f := func(msg *sarama.ConsumerMessage, elapsed time.Duration) func() {
					return func() {
						pmc.processMessage(msg, handMsg, elapsed)
						pmc.timersMux.Lock()
						delete(pmc.timers[msg.Topic][msg.Partition], msg.Offset)
						pmc.timersMux.Unlock()
					}
				}
				pmc.timersMux.Lock()
				timer := time.AfterFunc(time.Until(sched), f(msg, elapsed))
				addTimer(pmc.timers, timer, msg.Topic, msg.Partition, msg.Offset)
				pmc.timersMux.Unlock()
			} else {
				pmc.wgMsgHand.Add(1)
				go pmc.processMessage(msg, handMsg, elapsed)
			}
    case <-signals:
			break ConsumerLoop
    }

		if pmc.nbtotal > 0 {
			pmc.procmsgs++
			if pmc.procmsgs == pmc.nbtotal {
				log.Println("Limit of message to consume reached")
				break ConsumerLoop
			}
		}
	}
}

func (pmc *ProducingMessageConsumer) Close() {
	log.Printf("Stop sleeping routines for retry messages...\n")
	var nbStopped int
	pmc.timersMux.Lock()
	for _, p := range pmc.timers {
		for _, o := range p {
			for _, t := range o {
				if t.Stop() {
					pmc.wgMsgHand.Done()
					nbStopped++
				}
			}
		}
	}
	pmc.timersMux.Unlock()
	log.Printf("Nb stopped sleeping routines: %d\n", nbStopped)
	log.Printf("Finish processing already handled messages...\n")
	pmc.wgMsgHand.Wait()
	pmc.producer.AsyncClose()
	pmc.wgOutAcks.Wait()

	if err := pmc.consumer.Close(); err != nil {
		log.Println(err)
	}

	pmc.inAcksHand.printNbMarkedOffsets()
}

func (pmc *ProducingMessageConsumer) publishDeadLetter(msg *sarama.ConsumerMessage) {
	pmc.producer.Input() <- &sarama.ProducerMessage{
		Topic: pmc.consTopic + "-dead",
		Value: sarama.ByteEncoder(msg.Value),
		Metadata: &inputMetadatas{
			Offset: msg.Offset,
			Partition: msg.Partition,
			Topic: msg.Topic,
			NbOutputs: 1,
		},
	}
}

func (pmc *ProducingMessageConsumer) processMessage(msg *sarama.ConsumerMessage, handMsg *Message, elapsed time.Duration) {
	defer func() {
		pmc.wgMsgHand.Done()
		if pmc.msgsLimit > 0 {
			pmc.cond.L.Lock()
			pmc.msgs--
			pmc.cond.Signal()
			pmc.cond.L.Unlock()
		}
	}()
	prodMetas := &inputMetadatas{
		Offset: msg.Offset,
		Partition: msg.Partition,
		Topic: msg.Topic,
	}
	before := pmc.lr.Now()
	outputs, err := pmc.msgHand.Process(handMsg.Value)
	if err != nil {
		log.Printf("Processing message failed: %v\n", err)
		if handMsg.Metas != nil {
			handMsg.Metas.Retry++
		} else {
			handMsg.Metas = &MessageMetadatas{
				MaxRetry: pmc.maxRetry,
				Retry: 0,
			}
		}

		if handMsg.Metas.Retry == handMsg.Metas.MaxRetry {
			log.Println("Publishing in dead letter: max retry reached")
			pmc.publishDeadLetter(msg)
			return
		}

		newTi :=  time.Now().Add(latencies[handMsg.Metas.Retry])
		handMsg.Metas.Schedule, err = newTi.MarshalText()
		if err != nil {
			log.Panicln(err)
		}

		outEncoded, err := pmc.msgHand.Marshal(&Message{
			Value: handMsg.Value,
			Metas: handMsg.Metas,
		})

		if err != nil {
			log.Panicln(err)
		}

		retryTopic := topicsByRetry[handMsg.Metas.Retry]
		log.Printf("Publish on retry queue: %s", retryTopic)
		prodMetas.NbOutputs = 1
		pmc.producer.Input() <- &sarama.ProducerMessage{
			Topic: retryTopic,
			Value: sarama.ByteEncoder(outEncoded),
			Metadata: prodMetas,
		}
	} else {
		// Produce outputs
		nbOutputs := len(outputs)
		pmc.lr.AddToCollection(float64(nbOutputs))
		//log.Printf("NbFences = %d (Input:'%s'p:'%d'o:'%d')\n", nbOutputs, msg.Topic, msg.Partition, msg.Offset)
		if nbOutputs == 0 {
			pmc.inAcksHand.ack(msg.Topic, msg.Partition, msg.Offset)
			return
		}
		prodMetas.NbOutputs = nbOutputs

		for _, out := range outputs {
			pmc.producer.Input() <- &sarama.ProducerMessage{
				Topic: pmc.prodTopic,
				Value: sarama.ByteEncoder(out),
				Metadata: prodMetas,
			}
		}
		pmc.lr.AddLatency(pmc.lr.SinceTime(before) + elapsed)
	}
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

// Helpers

func addTimer(t map[string]map[int32]map[int64]*time.Timer, timer *time.Timer, topic string, part int32, off int64) {
	p, ok := t[topic]
	if !ok {
		p = make(map[int32]map[int64]*time.Timer)
		t[topic] = p
	}

	var o map[int64]*time.Timer
	o, ok = p[part]
	if !ok {
		o = make(map[int64]*time.Timer)
		p[part] = o
	}
	o[off] = timer
}

func getRetryTopicNames(topName string) []string {
	var topics []string

	m := make(map[time.Duration][]int)
	for i, lat := range latencies {
		m[lat] = append(m[lat], i)
	}

	for key, val := range m {
		var valStr string
		if key < time.Second {
			valStr = strconv.FormatFloat(float64(key) / float64(time.Second), 'f', -1, 64)
		} else {
			valStr = strconv.FormatInt(int64(key / time.Second), 10)
		}
		name := topName + "-" + valStr
		topics = append(topics, name)
		for _, ind := range val {
			topicsByRetry[ind] = name
		}
	}

	return topics
}
