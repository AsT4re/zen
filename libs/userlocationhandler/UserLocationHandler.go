package userlocationhandler

import (
	objects  "github.com/AsT4re/zen/libs/objects"
	pmc      "github.com/AsT4re/zen/libs/producingmessageconsumer"
	dgclient "github.com/AsT4re/zen/libs/dgclient"
	proto    "github.com/golang/protobuf/proto"
	errors   "github.com/pkg/errors"
)

type UserLocationHandler struct {
	DgCl *dgclient.DGClient
}

func (ulh *UserLocationHandler) Unmarshal(input []byte) (*pmc.Message, error) {
	userLoc := &objects.UserLocation {}
	if err := proto.Unmarshal(input, userLoc); err != nil {
		return nil, errors.Wrap(err, "Fail to get UserLocation object")
	}

	return &pmc.Message{
		Value: userLoc.GetValue(),
		Metas: userLoc.GetMetas(),
	}, nil
}

func (ulh *UserLocationHandler) Marshal(input *pmc.Message) ([]byte, error) {
	data, err := proto.Marshal(&objects.UserLocation{
		Metas: input.Metas,
		Value: input.Value.(*objects.UserLocationValue),
	})
	if err != nil {
		return nil, errors.Wrap(err, "Fail to marshall user location")
	}
	return data, nil
}

func (ulh *UserLocationHandler) Process(input interface{}) ([][]byte, error) {
	userLoc := input.(*objects.UserLocationValue)
	fences, err := ulh.DgCl.GetFencesContainingPos(userLoc.Long, userLoc.Lat)
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
