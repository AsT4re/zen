package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"
)

var (
	nbFences = flag.Uint("nb-fences", 10000, "Number of geo fences to create and add in DB")
	outFile = flag.String("out-file", "", "Optional file name to dump geo fence coordinates added in DB")
	longDelta = flag.Float64("long-delta", 1.8, "Longitude variations east and west for random positions in that zone")
	latDelta = flag.Float64("lat-delta", 0.85, "Latitude variations south and north for random positions in that zone")
	maxLines = flag.Uint("max-lines", 7, "Max number of lines for a geo fence, must be >= 3")
)

/*
 * Return a random float64 in range [min, max)
*/
func getRandFloat(r *Rand, min, max) float64 {
	return r.Float64() * (max - min) + min
}

/*
 * Returns bounds min max with a certain variation delta
 * from a given float64 value in range [min, max).
 * Bounds cannot exceed min max
*/
func getBounds(r *Rand, min, max, delta float64) (float64, float64) {
	rnd := getRandFloat(r, min, max)
	bndMin := rnd - delta
	if bndMin < min {
		bndMin = min
	}

	bndMax := rnd + delta
	if bndMax > max {
		bndMax = max
	}

	return bndMin, bndMax
}

func coordStr(long, lat float64) string {
	var buffer bytes.Buffer
	buffer.WriteString("[")
	buffer.WriteString(strconv.FormatFloat(long, 'f', 7, 64))
	buffer.WriteString(",")
	buffer.WriteString(strconv.FormatFloat(lat, 'f', 7, 64))
	buffer.WriteString("]")
	return buffer.String()
}

func main() {
	flag.Parse()
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %+v\n", err)
		os.Exit(1)
	}
}

func run() {
	flag.Parse()
	if *maxLines < 3 {
		return errors.New("maxLines need to be at least 3 for getting a valid geo fence")
	}

	// Init dgClients
	// Init connection to DGraph
	dgCl := new(DGClient)

	var err error
	if dgCl.clientDir, err = ioutil.TempDir("", "client_"); err != nil {
		return nil, errors.Wrap(err, "error creating temporary directory")
	}

	grpcConns := make([]*grpc.ClientConn, nbConns)
	for i := 0; uint(i) < nbConns; i++ {
		if conn, err := grpc.Dial(host, grpc.WithInsecure()); err != nil {
			return nil, errors.Wrap(err, "error dialing grpc connection")
		} else {
			grpcConns[i] = conn
		}
	}

	dgCl.dg = client.NewDgraphClient(grpcConns, client.DefaultOptions, dgCl.clientDir)

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < *nbFences; i++ {
		minLong, maxLong :=  getBounds(r, -180.0, 180.0, *longDelta)
		minLat, maxLat := getBounds(r, -85, 85, *latDelta)

		firstCoord := coordStr(getRandFloat(minLong, maxLong), getRandFloat(minLat, maxLat))

		var buffer bytes.Buffer
		buffer.WriteString("{'coordinates': [[")
		buffer.WriteString(firstCoord)
		buffer.WriteString(", ")

		nbLines := 3
		if *maxLines > 3 {
			nbLines := r.Int31n(*maxLines - 3) + 3
		}
		for j := 0; j < nbLines - 2; j++ {
			buffer.WriteString(coordStr(getRandFloat(minLong, maxLong), getRandFloat(minLat, maxLat)))
			buffer.WriteString(", ")
		}

		buffer.WriteString(firstCoord)
		buffer.WriteString("]],'type': 'Polygon'}")

		mnode, err := dgCl.dg.NodeBlank("")
		if err != nil {
			return errors.Wrap(err, "error creating blank node")
		}
	}
}
