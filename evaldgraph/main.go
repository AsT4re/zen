package main

import(
	pmc      "astare/zen/libs/producingmessageconsumer"
	ulh      "astare/zen/libs/userlocationhandler"
	rec      "astare/zen/libs/latencyrecorder"
	dbinit   "astare/zen/libs/dbinit"
	dgclient "astare/zen/libs/dgclient"
	lp       "astare/zen/libs/locationsproducer"
	errors   "github.com/pkg/errors"
	"flag"
	"fmt"
	"log"
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
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %+v\n", err)
		os.Exit(1)
	}
}

func run() error {
	if *topicUserLoc == "" {
		return errors.New("missing mandatory flag 'topic-user-loc'")
	}

	if *topicUserFence == "" {
		return errors.New("missing mandatory flag 'topic-user-fence'")
	}

	if len(dgHost) == 0 {
		dgHost = append(dgHost, "127.0.0.1:9080")
	}

	if err := lp.ProduceRandomLocations(*topicUserLoc, 10000, 0); err != nil {
		return err
	}

	dgLr := rec.NewLatencyRecorder(*dgAnalysis)
	dgCl, err := dgclient.NewDGClient(dgHost, *dgNbConns, dgLr)
	if err != nil {
		return err
	}
	if err := dgCl.Init(); err != nil {
		return err
	}
	defer dgCl.Close()

	if err := dbinit.AddRandomFences(dgCl, 100000, nil); err != nil {
		return err
	}

	log.Println("Fences added with success !")

	msgLr := rec.NewLatencyRecorder(*msgAnalysis)
	producingMsgCons, err := pmc.NewProducingMessageConsumer([]string{"localhost:9092"},
		                                                       *groupId,
		                                                       *topicUserLoc,
		                                                       *topicUserFence,
		                                                       &ulh.UserLocationHandler {
																														 DgCl: dgCl,
																													 },
		                                                       uint32(*maxRetry),
		                                                       *msgsLimit,
		                                                       *nbtotal,
		                                                       msgLr)

	if err != nil {
		return err
	}

	producingMsgCons.Consume()
	producingMsgCons.Close()

	if *dgAnalysis || *msgAnalysis {
		f, err := os.OpenFile("analysis", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return errors.Wrap(err, "Fail to open file")
		}
		defer f.Close()

		dgLr.DumpFullAnalysis(f, fmt.Sprintf("DGraph (parallel reqs: %d): \n", *msgsLimit))
		msgLr.DumpFullAnalysis(f, fmt.Sprintf("Msg (parallel process: %d): \n", *msgsLimit))
	}

	return nil
}

