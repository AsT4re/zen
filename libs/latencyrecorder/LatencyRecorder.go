package latencyrecorder

import(
	stats "github.com/montanaflynn/stats"
	"sync"
	"time"
	"log"
	"os"
	"strconv"
)

type LatencyRecorder struct {
	coll       []float64
	collName   string
	collUnit   string
	collMut    sync.Mutex
	reqLats    []float64
	reqLatsMut sync.Mutex
	enabled    bool
}

func NewLatencyRecorder(enabled bool) *LatencyRecorder {
	return &LatencyRecorder {
		enabled: enabled,
	}
}

func (lr *LatencyRecorder) SetCollectionSpecs(name string, unit string) {
	if lr.enabled == true {
		lr.collName = name
		lr.collUnit = unit
	}
}

func (lr *LatencyRecorder) Now() time.Time {
	if lr.enabled == true {
		return time.Now()
	}
	return time.Time{}
}

func (lr *LatencyRecorder) SinceTime(from time.Time) time.Duration {
	if lr.enabled == true {
		return time.Since(from)
	}
	return 0
}

func (lr *LatencyRecorder) AddToCollection(elem float64) {
	if lr.enabled == true {
		lr.coll = addElemToColl(lr.coll, elem, &lr.collMut)
	}
}

func (lr *LatencyRecorder) AddLatency(lat time.Duration) {
	if lr.enabled == true {
		lr.reqLats = addElemToColl(lr.reqLats, lat.Seconds()*1000, &lr.reqLatsMut)
	}
}

func (lr *LatencyRecorder) Reset() {
	lr.coll = nil
	lr.reqLats = nil
}

func (lr *LatencyRecorder) DumpFullAnalysis(f *os.File, title string) {
	if lr.enabled == true {
		f.WriteString(title + "\n")
		if lr.reqLats != nil {
			dumpCollecAnalalysis(lr.reqLats, f, "Latencies", "ms")
		}

		if lr.coll != nil {
			dumpCollecAnalalysis(lr.coll, f, lr.collName, lr.collUnit)
		}
	}
}

func addElemToColl(coll []float64, elem float64, mut *sync.Mutex) []float64 {
	mut.Lock()
	defer mut.Unlock()
	return append(coll, elem)
}

func dumpCollecAnalalysis(collec []float64, f *os.File, title string, unit string) {
	f.WriteString(title + "\n")
	mean, err := stats.Mean(collec)
	if err != nil {
		log.Printf("Error computing Mean: %v\n", err)
	} else {
		f.WriteString("Mean: " + strconv.FormatFloat(mean, 'f', -1, 64) + unit + "\n")
	}

	perc, err := stats.Percentile(collec, 95.0)
	if err != nil {
		log.Printf("Error computing Percentile 95: %v\n", err)
	} else {
		f.WriteString("Percentile 95: " + strconv.FormatFloat(perc, 'f', -1, 64) + unit + "\n")
	}
}
