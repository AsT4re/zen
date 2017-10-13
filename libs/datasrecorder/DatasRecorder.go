package datasrecorder

import(
	stats  "github.com/montanaflynn/stats"
	errors "github.com/pkg/errors"
	"sync"
	"time"
	"log"
)

type Stats struct {
	Mean    float64
	Perc95  float64
}

type Collection struct {
	coll    []float64
	stats   *Stats
	mut     sync.Mutex
}

type DatasRecorder struct {
	colls   map[string]*Collection
	enabled bool
}

func NewDatasRecorder(enabled bool) *DatasRecorder {
	dr := &DatasRecorder {
		enabled: enabled,
	}

	if enabled == true {
		dr.colls = make(map[string]*Collection)
	}

	return dr
}

func (dr *DatasRecorder) NewCollection(name string) {
	if dr.enabled == true {
		if _, ok := dr.colls[name]; ok == true {
			log.Printf("Collection %s already exists\n", name)
			return
		}

		dr.colls[name] = &Collection{}
	}
}

func (dr *DatasRecorder) AddToCollection(name string, elem float64) {
	if dr.enabled == true {
		c, ok := dr.colls[name]
		if ok == false {
			log.Printf("Collection %s does not exist\n", name)
			return
		}
		c.mut.Lock()
		c.coll = append(c.coll, elem)
		c.mut.Unlock()
	}
}

func (dr *DatasRecorder) Reset() {
	if dr.enabled == true {
		for _, c := range dr.colls {
			c.coll = nil
			c.stats = nil
		}
	}
}

func (dr *DatasRecorder) Now() time.Time {
	if dr.enabled == true {
		return time.Now()
	}
	return time.Time{}
}

func (dr *DatasRecorder) SinceTime(from time.Time) time.Duration {
	if dr.enabled == true {
		return time.Since(from)
	}
	return 0
}

func (dr *DatasRecorder) GetStats(name string) (Stats,error) {
	c, ok := dr.colls[name]
	if ok == false {
		return Stats{}, errors.New("Collection does not exist")
	}

	if c.stats == nil {
		var (
			err    error
			mean   float64
			perc95 float64
		)

		if mean, err = stats.Mean(c.coll); err != nil {
			return Stats{}, errors.Wrap(err, "Error computing mean")
		}
		if perc95, err = stats.Percentile(c.coll, 95.0); err != nil {
			return Stats{}, errors.Wrap(err, "Error computing percentile 95")
		}
		c.stats = &Stats{
			Mean: mean,
			Perc95: perc95,
		}
	}

	return *c.stats, nil
}
