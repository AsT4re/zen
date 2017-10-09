package main

import (
	dgclient "astare/zen/dgclient"
	objects  "astare/zen/objects"
	pmc      "astare/zen/producingmessageconsumer"
	rec      "astare/zen/latencyrecorder"
	proto    "github.com/golang/protobuf/proto"
	errors   "github.com/pkg/errors"
)

type userLocationHandler struct {
	dgCl *dgclient.DGClient
}

func NewUserLocationHandler(dgNbConns uint, dgHost string, lr *rec.LatencyRecorder) (*userLocationHandler, error) {

	handler := new(userLocationHandler)
	// Create client + init connection
	var err error
	handler.dgCl, err = dgclient.NewDGClient(dgHost, dgNbConns, lr)
	if err != nil {
		return nil, err
	}
	if err := handler.dgCl.Init(); err != nil {
		return nil, err
	}

	return handler, nil
}

func (ulh *userLocationHandler) Unmarshal(input []byte) (*pmc.Message, error) {
	userLoc := &objects.UserLocation {}
	if err := proto.Unmarshal(input, userLoc); err != nil {
		return nil, errors.Wrap(err, "Fail to get UserLocation object")
	}

	return &pmc.Message{
		Value: userLoc.GetValue(),
		Metas: userLoc.GetMetas(),
	}, nil
}

func (ulh *userLocationHandler) Marshal(input *pmc.Message) ([]byte, error) {
	data, err := proto.Marshal(&objects.UserLocation{
		Metas: input.Metas,
		Value: input.Value.(*objects.UserLocationValue),
	})
	if err != nil {
		return nil, errors.Wrap(err, "Fail to marshall user location")
	}
	return data, nil
}

func (ulh *userLocationHandler) Process(input interface{}) ([][]byte, error) {
	userLoc := input.(*objects.UserLocationValue)
	fences, err := ulh.dgCl.GetFencesContainingPos(userLoc.Long, userLoc.Lat)
	if err != nil {
		return nil, err
	}

	nbFences := len(fences.Root)

	if nbFences == 0 {
		return [][]byte{}, nil
	}

	var outputs [][]byte
	for _, fence := range fences.Root {
		newFence := &objects.UserFence {
			UserId: userLoc.GetUserId(),
			PlaceName: fence.Name,
		}

		data, err := proto.Marshal(newFence)
		if err != nil {
			return nil, errors.Wrap(err, "Fail to marshall fence")
		}
		outputs = append(outputs, data)
	}

	return outputs, nil
}

func (ulh *userLocationHandler) Close() {
	ulh.dgCl.Close()
}
