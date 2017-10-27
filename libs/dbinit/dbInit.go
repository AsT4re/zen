package dbinit

import (
	"astare/zen/libs/dgclient"
	"astare/zen/libs/randcoords"
	"bytes"
	"github.com/pkg/errors"
	randomdata "github.com/Pallinder/go-randomdata"
	"encoding/json"
	"os"
	"math/rand"
	"time"
	"log"
)

type geometry struct {
	Type        string         `json:"type"`
	Coordinates [][][]float64  `json:"coordinates"`
}

type AddFencesConfig struct {
	Seed      int64
	OutFile   string
	LongDelta float64
	LatDelta  float64
	MaxLines  int
}

func DefaultConfig() *AddFencesConfig {
	return &AddFencesConfig{
		OutFile: "",
		Seed: 0,
		LongDelta: 0.9,
		LatDelta: 0.425,
		MaxLines: 4,
	}
}

func AddRandomFences(dgCl *dgclient.DGClient, nbFences int, config *AddFencesConfig) error {
	if config == nil {
		config = DefaultConfig()
	}

	if config.MaxLines < 3 {
		return errors.New("maxLines need to be at least 3 for getting a valid geo fence")
	}

	var f *os.File
	if config.OutFile != "" {
		var err error
		f, err = os.Create(config.OutFile)
		if err != nil {
			return errors.Wrap(err, "Error creating file")
		}
		defer f.Close()
	}

	if config.Seed == 0 {
		config.Seed = time.Now().UnixNano()
	}

	r := rand.New(rand.NewSource(config.Seed))

	for i := 0; i < nbFences; i++ {
		minLong, maxLong :=  randcoords.GetBounds(r, -180, 180, config.LongDelta)
		minLat, maxLat := randcoords.GetBounds(r, -85, 85, config.LatDelta)

		var coords [][]float64

		nbLines := 3
		if config.MaxLines > 3 {
			nbLines = int(r.Int31n(int32(config.MaxLines) - 2) + 3)
		}

		longDiv := int(float64(nbLines + 1) / 2.0)
		vertLongDelta := (maxLong - minLong) / float64(longDiv)
		vertLatDelta := (maxLat - minLat) / 2

		var firstCoord []float64

		maxLong = minLong + vertLongDelta
		minLat += vertLatDelta
		for i := 0; i < longDiv; i++ {
			genCoord := []float64 {randcoords.GetRandCoord(r, minLong, maxLong), randcoords.GetRandCoord(r, minLat, maxLat),}
			if i == 0 {
				firstCoord = genCoord
			}
			coords = append(coords, genCoord)
			if i != longDiv - 1 {
				minLong += vertLongDelta
				maxLong += vertLongDelta
			}
		}

		maxLat = minLat
		minLat = maxLat - vertLatDelta
		for i := longDiv; i < nbLines; i++ {
			coords = append(coords, []float64 {randcoords.GetRandCoord(r, minLong, maxLong), randcoords.GetRandCoord(r, minLat, maxLat),})
			if i == nbLines - 2 && nbLines % 2 == 1 {
				minLong -= 2 * vertLongDelta
				maxLong -= 2 * vertLongDelta
			} else {
				minLong -= vertLongDelta
				maxLong -= vertLongDelta
			}
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
		if err := dgCl.AddNewNodeToBatch(randomdata.SillyName(), locStr, now); err != nil {
			return err
		}

		if i != 0 && i % 10000 == 0 {
			log.Printf("%d fences added\n", i)
		}
	}

	dgCl.BatchFlush()
	return nil
}
