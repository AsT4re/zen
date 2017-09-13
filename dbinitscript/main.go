package main

import (
	"astare/zen/dgclient"
	"astare/zen/randcoords"
	"bytes"
	"github.com/pkg/errors"
	"flag"
	"fmt"
	randomdata "github.com/Pallinder/go-randomdata"
	"encoding/json"
	"os"
	"math/rand"
	"time"
)

var (
	nbFences = flag.Uint("nb-fences", 10000, "Number of geo fences to create and add in DB")
	outFile = flag.String("out-file", "", "Optional file name to dump geo fence coordinates added in DB")
	longDelta = flag.Float64("long-delta", 0.9, "Longitude variations east and west for random positions in that zone")
	latDelta = flag.Float64("lat-delta", 0.425, "Latitude variations south and north for random positions in that zone")
	maxLines = flag.Int("max-lines", 4, "Max number of lines for a geo fence, must be >= 3")
	dgHost = flag.String("dg-host-and-port", "127.0.0.1:9080", "Dgraph database hostname and port")
	dgNbConns = flag.Uint("dg-conns-pool", 100, "Number of connections to DGraph")
	seed = flag.Int64("seed", 0, "Seed for generation of random fences, use same seed for same sequence")
)

type geometry struct {
	Type        string         `json:"type"`
	Coordinates [][][]float64  `json:"coordinates"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %+v\n", err)
		os.Exit(1)
	}
}



func run() error {
	flag.Parse()
	if *maxLines < 3 {
		return errors.New("maxLines need to be at least 3 for getting a valid geo fence")
	}

	var f *os.File
	if *outFile != "" {
		var err error
		f, err = os.Create(*outFile)
		if err != nil {
			return errors.Wrap(err, "Error creating file")
		}
		defer f.Close()
	}

	// Init connection to DGraph
	dgcl, err := dgclient.NewDGClient(*dgHost, *dgNbConns)
	if err != nil {
		return err
	}
	if err := dgcl.Init(); err != nil {
		return err
	}
	defer dgcl.Close()

	r := rand.New(rand.NewSource(*seed))

	for i := uint(0); i < *nbFences; i++ {
		minLong, maxLong :=  randcoords.GetBounds(r, -180, 180, *longDelta)
		minLat, maxLat := randcoords.GetBounds(r, -85, 85, *latDelta)

		var coords [][]float64

		firstCoord := []float64 {
			randcoords.GetRandCoord(r, minLong, maxLong),
			randcoords.GetRandCoord(r, minLat, maxLat),
		}

		coords = append(coords, firstCoord)

		nbLines := 3
		if *maxLines > 3 {
			nbLines = int(r.Int31n(int32(*maxLines) - 2) + 3)
		}
		for j := 0; j < nbLines - 1; j++ {
			coords = append(coords, []float64 {randcoords.GetRandCoord(r, minLong, maxLong), randcoords.GetRandCoord(r, minLat, maxLat),})
		}

		coords = append(coords, firstCoord)

		polyg := geometry {
			"Polygon",
			[][][]float64 {},
		}

		polyg.Coordinates = append(polyg.Coordinates, coords)

		buf := bytes.Buffer{}
		if err := json.NewEncoder(&buf).Encode(polyg); err != nil {
			return errors.Wrap(err, "fail to encode polyg to json")
		}
		locStr := buf.String()

		if f != nil {
			f.WriteString(locStr)
			f.WriteString("\n")
		}

		now := time.Now()
		if err := dgcl.AddNewNodeToBatch(randomdata.SillyName(), locStr, now); err != nil {
			return err
		}
	}

	fmt.Printf("Flushing batch...\n")
	dgcl.BatchFlush()
	fmt.Printf("Flushing batch finished\n")

	return nil
}
