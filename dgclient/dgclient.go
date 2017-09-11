package dgclient

import (
	"context"
	"github.com/dgraph-io/dgraph/client"
	"github.com/pkg/errors"
	"fmt"
	"google.golang.org/grpc"
	"io/ioutil"
	"os"
	"time"
)

type DGClient struct {
	conns     []*grpc.ClientConn
	clientDir string
	dg        *client.Dgraph
}

func NewDGClient(host string, nbConns uint) (*DGClient, error) {
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
		return err
	}
	if err = addEdge(dgCl, &mnode, "name", name); err != nil {
		return err
	}
	if err = addEdge(dgCl, &mnode, "created_at", created_at); err != nil {
		return err
	}

	return nil
}

func (dgCl *DGClient) BatchFlush() {
	dgCl.dg.BatchFlush()
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
