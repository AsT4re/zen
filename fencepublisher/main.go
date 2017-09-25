package main

import(
	pmc "astare/zen/ProducingMessageConsumer"
	"flag"
	"log"
)

var (
	topicUserLoc = flag.String("topic-user-loc", "", "Topic where to consume UserLocation messages")
	topicUserFence = flag.String("topic-user-fence", "", "Topic where to produce UserFence messages")
	dgNbConns = flag.Uint("dg-conns-pool", 10, "Number of connections to DGraph")
	dgHost = flag.String("dg-host-and-port", "127.0.0.1:9080", "Dgraph database hostname and port")
	groupId = flag.String("group-id", "user-loc", "Group id for consumer cluster")
)

func main() {
	flag.Parse()

	if *topicUserLoc == "" {
		log.Fatalln("missing mandatory flag 'topic-user-loc'")
	}

	if *topicUserFence == "" {
		log.Fatalln("missing mandatory flag 'topic-user-fence'")
	}

	msgHandler, err := NewUserLocationHandler(*dgNbConns, *dgHost)
	if err != nil {
		log.Fatalln(err)
	}

	defer msgHandler.Close()

	producingMsgCons, err := pmc.NewProducingMessageConsumer([]string{"localhost:9092"},
		                                                       *groupId,
		                                                       *topicUserLoc,
		                                                       *topicUserFence,
		                                                       msgHandler)

	if err != nil {
		log.Fatalln(err)
	}

	defer producingMsgCons.Close()
	producingMsgCons.Consume()
}

