package main

import(
	pmc      "astare/zen/libs/producingmessageconsumer"
	ulh      "astare/zen/libs/userlocationhandler"
	rec      "astare/zen/libs/datasrecorder"
	dbinit   "astare/zen/libs/dbinit"
	dgclient "astare/zen/libs/dgclient"
	lp       "astare/zen/libs/locationsproducer"
	sfd      "astare/zen/libs/statsfiledatas"
	arrfl    "astare/zen/libs/arrstrflags"
	errors   "github.com/pkg/errors"
	"flag"
	"fmt"
	"log"
	"os"
	"time"
)

var (
	topicUserLoc = flag.String("topic-user-loc", "", "Topic where to consume UserLocation messages")
	topicUserFence = flag.String("topic-user-fence", "", "Topic where to produce UserFence messages")
	dgHost arrfl.ArrStrFlags
	dgNbConns = flag.Uint("dg-conns-pool", 1000, "Number of connections to DGraph")
	statsFile = flag.String("stats-file", "no-name", "Title for the test (output file name)")
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
	dgDr := rec.NewDatasRecorder(true)
	dgCl, err := dgclient.NewDGClient(dgHost, *dgNbConns, dgDr)
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
	msgDr := rec.NewDatasRecorder(true)
	producingMsgCons, err := pmc.NewProducingMessageConsumer([]string{"localhost:9092"},
		                                                       "user-loc",
		                                                       *topicUserLoc,
		                                                       *topicUserFence,
		                                                       &ulh.UserLocationHandler {
																														 DgCl: dgCl,
																													 },
		                                                       uint32(0),
		                                                       msgDr)

	defer producingMsgCons.Close()

	batchConf := dbinit.DefaultConfig()
	fenDiv := 10000

	rndLocsSeed := int64(0)

	fDatas := sfd.NewStatsFilesDatas()
	fDatas.Vars["nb-fences"] = []float64{100000, 100000, 300000, 500000, 1000000}
	fDatas.Vars["parallel-conns"] = []float64{1, 2, 5, 10, 15, 20, 25, 35, 50, 75, 100, 125, 150, 175, 200, 250, 300, 400, 500}
	statsNames := []string{
		"throughput",
		"dg-lat-mean",
		"dg-lat-perc95",
		"msg-lat-mean",
		"msg-lat-perc95",
		"fences-mean",
		"fences-perc95",
	}

	if err := fDatas.InitStats(statsNames); err != nil {
		return err
	}

	nbFencesAcc := 0
	j := 0
	nbFencesArr := fDatas.Vars["nb-fences"]
	for i, nbFenTmp := range nbFencesArr {
		nbFen := int(nbFenTmp)
		batchConf.Seed++
		nbFencesAcc += nbFen
		log.Printf("Adding %d random fences in dgraph...\n", nbFen)
		if i != 0 {
			if err := dgCl.ResetClient(); err != nil {
				return err
			}
		}
		if err := dbinit.AddRandomFences(dgCl, nbFen, batchConf); err != nil {
			return err
		}

		timeToWait := nbFen/fenDiv + 60
		log.Printf("Fences added with success. Waiting %d seconds for data to be replicated...\n", timeToWait)
		time.Sleep(time.Duration(timeToWait) * time.Second)

		parallelConns := fDatas.Vars["parallel-conns"]
		for _, paralTmp := range parallelConns {
			paral := int(paralTmp)
			log.Printf("Adding %d random location messages on %s topic...\n", nbtotal, *topicUserLoc)
			rndLocsSeed++
			if err := lp.ProduceRandomLocations(*topicUserLoc, nbtotal, rndLocsSeed); err != nil {
				return err
			}

			log.Printf("Consuming and processing %d messages with a limit of %d parallel messages...\n", nbtotal, paral)
			before := time.Now()
			producingMsgCons.Consume(nbtotal, paral)
			producingMsgCons.WaitProcessingMessages()
			dur := time.Since(before)

			fDatas.Stats["throughput"][j] = float64(nbtotal) / dur.Seconds()

			if err := addStats(dgDr, "latencies", fDatas.Stats["dg-lat-mean"], fDatas.Stats["dg-lat-perc95"], j); err != nil {
				return err
			}

			if err := addStats(msgDr, "latencies", fDatas.Stats["msg-lat-mean"], fDatas.Stats["msg-lat-perc95"], j); err != nil {
				return err
			}
			if err := addStats(msgDr, "fences", fDatas.Stats["fences-mean"], fDatas.Stats["fences-perc95"], j); err != nil {
				return err
			}

			j++

			dgDr.Reset()
			msgDr.Reset()
		}
	}

	fDatas.Serialize(*statsFile)

	return nil
}

func addStats(dr *rec.DatasRecorder, name string, slMeans []float64, slPerc95s []float64, j int) error {
	stats, err := dr.GetStats(name)
	if err != nil {
		return err
	}

	slMeans[j] = stats.Mean
	slPerc95s[j] = stats.Perc95
	return nil
}
