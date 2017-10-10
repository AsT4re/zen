package dgclient

import (
	client "github.com/dgraph-io/dgraph/client"
	errors "github.com/pkg/errors"
	rec    "astare/zen/libs/latencyrecorder"
	"bytes"
	"strconv"
	"context"
	"fmt"
	"google.golang.org/grpc"
	"io/ioutil"
	"os"
	"time"
)

/*
 * Public structures
 */

type DGClient struct {
	conns     []*grpc.ClientConn
	clientDir string
	dg        *client.Dgraph
	lr        *rec.LatencyRecorder
}

type FenceProps struct {
	Name        string        `json:"name"`
}

// Reply structure from GetFencesContainingPos request
type FencesRep struct {
	Root        []*FenceProps `json:"fences"`
}

func NewDGClient(host []string, nbConns uint, lr *rec.LatencyRecorder) (*DGClient, error) {
	// Init connection to DGraph
	lenHosts := len(host)
	if lenHosts == 0 || nbConns == 0 {
		return nil, errors.New("At least 1 host and 1 connection is needed")
	}

	dgCl := &DGClient{
		lr: lr,
	}

	if lr == nil {
		dgCl.lr = rec.NewLatencyRecorder(false)
	}

	var err error
	if dgCl.clientDir, err = ioutil.TempDir("", "client_"); err != nil {
		return nil, errors.Wrap(err, "error creating temporary directory")
	}

	grpcConns := make([]*grpc.ClientConn, nbConns)
	for i := 0; uint(i) < nbConns; i++ {
		if conn, err := grpc.Dial(host[i % lenHosts], grpc.WithInsecure()); err != nil {
			return nil, errors.Wrap(err, "error dialing grpc connection")
		} else {
			grpcConns[i] = conn
		}
	}

	dgCl.dg = client.NewDgraphClient(grpcConns, client.DefaultOptions, dgCl.clientDir)

	return dgCl, nil
}

// Initialize DB with schema
func (dgCl *DGClient) Init() error {
	req := client.Req{}
	req.SetQuery(`
    mutation {
      schema {
        loc: geo @index(geo) .
        name: string .
        created_at: dateTime .
      }
    }
`)

	if _, err := dgCl.dg.Run(context.Background(), &req); err != nil {
		return errors.Wrap(err, "error running request for schema")
	}

	return nil
}

func (dgCl *DGClient) Close() {
	if len(dgCl.conns) > 0 {
		connsLen := len(dgCl.conns)
		for i := 0; i < connsLen; i++ {
			if err := dgCl.conns[i].Close(); err != nil {
				fmt.Fprintf(os.Stderr, "WARNING: %+v\n", errors.Wrap(err, "closing connection failed:"))
			}
		}
	}

	if dgCl.clientDir != "" {
		if err := os.RemoveAll(dgCl.clientDir); err != nil {
			fmt.Fprintf(os.Stderr, "WARNING: %+v\n", errors.Wrap(err, "removing temp dir failed:"))
		}
	}
}

func (dgCl *DGClient) AddNewNodeToBatch(name, loc string, created_at time.Time) error {
	mnode, err := dgCl.dg.NodeBlank("")
	if err != nil {
		return errors.Wrap(err, "error creating blank node")
	}

	if err = addEdge(dgCl, &mnode, "loc", loc); err != nil {
		return errors.Wrap(err, "error when adding node to batch")
	}
	if err = addEdge(dgCl, &mnode, "name", name); err != nil {
		return errors.Wrap(err, "error when adding node to batch")
	}
	if err = addEdge(dgCl, &mnode, "created_at", created_at); err != nil {
		return errors.Wrap(err, "error when adding node to batch")
	}

	return nil
}

func (dgCl *DGClient) BatchFlush() {
	dgCl.dg.BatchFlush()
}

func (dgCl *DGClient) GetFencesContainingPos(long, lat float64) (FencesRep, error) {
	getFencesTempl := `{
    fences(func: contains(loc, $pos)) {
      name
    }
  }`

	var buffer bytes.Buffer
	buffer.WriteString("[")
	buffer.WriteString(strconv.FormatFloat(long, 'f', -1, 64))
	buffer.WriteString(",")
	buffer.WriteString(strconv.FormatFloat(lat, 'f', -1, 64))
	buffer.WriteString("]")

	reqMap := make(map[string]string)
	reqMap["$pos"] = buffer.String()

	var fences FencesRep
	err := sendRequest(dgCl, &getFencesTempl, &reqMap, &fences)
	return fences, errors.Wrap(err, "error when sending request to dgraph")
}

/*
 *  Private functions
 */

func addEdge(dgCl *DGClient, mnode *client.Node, name string, value interface{}) error {
	e := mnode.Edge(name)
	var err error
	switch v := value.(type) {
	case int64:
		err = e.SetValueInt(v)
	case string:
		if name == "loc" {
			err = e.SetValueGeoJson(v)
		} else {
			err = e.SetValueString(v)
		}
	case time.Time:
		err = e.SetValueDatetime(v)
	case float64:
		err = e.SetValueFloat(v)
	default:
		return errors.New("Type for value not handled yet")
	}

	if err != nil {
		return errors.Wrapf(err, "error when setting value for '%v' edge with value '%v'", name, value)
	}

	if err = dgCl.dg.BatchSet(e); err != nil {
		return errors.Wrapf(err, "error when setting edge '%v' with value '%v' for batch", name, value)
	}

	return nil
}

func sendRequest(dgCl *DGClient, reqStr *string, reqMap *map[string]string, rep interface{}) error {
	req := client.Req{}
	req.SetQueryWithVariables(*reqStr, *reqMap)


	before := dgCl.lr.Now()
	resp, err := dgCl.dg.Run(context.Background(), &req)
	elapsed := dgCl.lr.SinceTime(before)
	if err != nil {
		return errors.Wrap(err, "error when executing request")
	}
	dgCl.lr.AddLatency(elapsed)

	if len(resp.N[0].Children) == 0 {
		return nil
	}

	if err = client.Unmarshal(resp.N, rep); err != nil {
		return errors.Wrap(err, "error when unmarshaling dgraph reply")
	}

	return nil
}