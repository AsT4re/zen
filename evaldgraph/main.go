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
	"time"
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

	// Initialize dgClient with latency analyzer
	dgLr := rec.NewLatencyRecorder(true)
	dgCl, err := dgclient.NewDGClient(dgHost, *dgNbConns, dgLr)
	if err != nil {
		return err
	}
	if err := dgCl.Init(); err != nil {
		return err
	}
	defer dgCl.Close()

	// Number of messages to process for each call to Consume()
	nbtotal := 100000
	// Initialize producing message consumer with latency analyzer
	msgLr := rec.NewLatencyRecorder(true)
	producingMsgCons, err := pmc.NewProducingMessageConsumer([]string{"localhost:9092"},
		                                                       "user-loc",
		                                                       *topicUserLoc,
		                                                       *topicUserFence,
		                                                       &ulh.UserLocationHandler {
																														 DgCl: dgCl,
																													 },
		                                                       uint32(0),
		                                                       msgLr)

	defer producingMsgCons.Close()

	f, err := os.OpenFile("analysis", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return errors.Wrap(err, "Fail to open file")
	}
	defer f.Close()

	batchConf := dbinit.DefaultConfig()
	nbFencesArr := []int{100000, 100000}
	fenDiv := 10000
	var nbFencesAcc int
	msgsLimit := []int{50, 100}
	rndLocsSeed := int64(1)

	for i, nbFences := range nbFencesArr {
		batchConf.Seed++
		nbFencesAcc += nbFences
		log.Printf("Adding %d random fences in dgraph...\n", nbFences)
		if i != 0 {
			if err := dgCl.ResetClient(); err != nil {
				return err
			}
		}
		if err := dbinit.AddRandomFences(dgCl, nbFences, batchConf); err != nil {
			fmt.Printf("HERE ERROR: %v\n", err)
			return err
		}

		timeToWait := nbFences/fenDiv + 5
		log.Printf("Fences added with success. Waiting %d seconds for data to be replicated...\n", timeToWait)
		time.Sleep(time.Duration(nbFences/fenDiv + 5) * time.Second)

		for _, limit := range msgsLimit {
			log.Printf("Adding %d random location messages on %s topic...\n", nbtotal, *topicUserLoc)
			if err := lp.ProduceRandomLocations(*topicUserLoc, nbtotal, rndLocsSeed); err != nil {
				return err
			}

			before := time.Now()
			producingMsgCons.Consume(nbtotal, limit)
			producingMsgCons.WaitProcessingMessages()
			dur := time.Since(before)

			throughput := float64(nbtotal) / dur.Seconds()
			f.WriteString(fmt.Sprintf("Stats (NbFences: %d. Parallel connections: %d):\n\n", nbFencesAcc, limit))
			f.WriteString(fmt.Sprintf("Throughput: %f/sec\n\n", throughput))
			dgLr.DumpFullAnalysis(f, "\nDgraph requests:\n")
			msgLr.DumpFullAnalysis(f, "\nConsumed messages:\n")
			f.WriteString("\n")

			dgLr.Reset()
			msgLr.Reset()
		}
	}

	return nil
}

