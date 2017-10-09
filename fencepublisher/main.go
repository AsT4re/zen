package main

import(
	pmc "astare/zen/producingmessageconsumer"
	rec "astare/zen/latencyrecorder"
	"flag"
	"log"
	"fmt"
	"os"
	"bytes"
)

type arrStrFlags []string

func (i *arrStrFlags) String() string {
	var buffer bytes.Buffer

	first := true
	for _, s := range *i {
		if first == false {
			buffer.WriteString(",")
		} else {
			first = false
		}
		buffer.WriteString(s)
	}

	return buffer.String()
}

func (i *arrStrFlags) Set(value string) error {
    *i = append(*i, value)
    return nil
}

var (
	topicUserLoc = flag.String("topic-user-loc", "", "Topic where to consume UserLocation messages")
	topicUserFence = flag.String("topic-user-fence", "", "Topic where to produce UserFence messages")
	dgHost arrStrFlags
	dgNbConns = flag.Uint("dg-conns-pool", 1000, "Number of connections to DGraph")
	groupId = flag.String("group-id", "user-loc", "Group id for consumer cluster")
	maxRetry = flag.Uint("max-retry", 6, "Retry value if processing message have failed")
	dgAnalysis = flag.Bool("dg-analysis", false, "Dgraph request latency analysis")
	msgAnalysis = flag.Bool("msg-analysis", false, "Latency analysis for processing one message")
	msgsLimit = flag.Int("msgs-limit", 1000, "Max number of concurrent messages to process")
	nbtotal = flag.Int("nb-total", 0, "Total number of messages to consume before ending consuming")
)

func main() {
	flag.Var(&dgHost, "dg-host-and-port", "Dgraph database hostname and port")
	flag.Parse()

	if len(dgHost) == 0 {
		dgHost = append(dgHost, "127.0.0.1:9080")
	}

	if *topicUserLoc == "" {
		log.Fatalln("missing mandatory flag 'topic-user-loc'")
	}

	if *topicUserFence == "" {
		log.Fatalln("missing mandatory flag 'topic-user-fence'")
	}

	dgLr := rec.NewLatencyRecorder(*dgAnalysis)
	msgHandler, err := NewUserLocationHandler(*dgNbConns, dgHost, dgLr)
	if err != nil {
		log.Fatalln(err)
	}

	defer msgHandler.Close()

	msgLr := rec.NewLatencyRecorder(*msgAnalysis)
	producingMsgCons, err := pmc.NewProducingMessageConsumer([]string{"localhost:9092"},
		                                                       *groupId,
		                                                       *topicUserLoc,
		                                                       *topicUserFence,
		                                                       msgHandler,
		                                                       uint32(*maxRetry),
		                                                       *msgsLimit,
		                                                       *nbtotal,
		                                                       msgLr)

	if err != nil {
		log.Fatalln(err)
	}

	producingMsgCons.Consume()
	producingMsgCons.Close()

	if *dgAnalysis || *msgAnalysis {
		f, err := os.OpenFile("analysis", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			log.Println("Error creating file: %v\n", err)
			return
		}
		defer f.Close()

		dgLr.DumpFullAnalysis(f, fmt.Sprintf("DGraph (parallel reqs: %d): \n", *msgsLimit))
		msgLr.DumpFullAnalysis(f, fmt.Sprintf("Msg (parallel process: %d): \n", *msgsLimit))
	}
}

