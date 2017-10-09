package main

import(
	pmc "astare/zen/producingmessageconsumer"
	rec "astare/zen/latencyrecorder"
	"flag"
	"log"
	"fmt"
	"os"
)

var (
	topicUserLoc = flag.String("topic-user-loc", "", "Topic where to consume UserLocation messages")
	topicUserFence = flag.String("topic-user-fence", "", "Topic where to produce UserFence messages")
	dgNbConns = flag.Uint("dg-conns-pool", 10, "Number of connections to DGraph")
	dgHost = flag.String("dg-host-and-port", "127.0.0.1:9080", "Dgraph database hostname and port")
	groupId = flag.String("group-id", "user-loc", "Group id for consumer cluster")
	maxRetry = flag.Uint("max-retry", 6, "Retry value if processing message have failed")
	msgsLimit = flag.Int("msgs-limit", 0, "Max number of concurrent messages to process. Default is unlimited")
	dgAnalysis = flag.Bool("dg-analysis", false, "Dgraph request latency analysis")
	msgAnalysis = flag.Bool("msg-analysis", false, "Latency analysis for processing one message")
	nbtotal = flag.Int("nb-total", 0, "Total number of messages to consume before ending consuming")
)

func main() {
	flag.Parse()

	if *topicUserLoc == "" {
		log.Fatalln("missing mandatory flag 'topic-user-loc'")
	}

	if *topicUserFence == "" {
		log.Fatalln("missing mandatory flag 'topic-user-fence'")
	}

	dgLr := rec.NewLatencyRecorder(*dgAnalysis)
	msgHandler, err := NewUserLocationHandler(*dgNbConns, *dgHost, dgLr)
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

