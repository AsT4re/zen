package pmc

type Message struct {
	Value interface{}
	Metas *MessageMetadatas
}

type MessageHandler interface {
	Unmarshal(input []byte) (*Message, error)
	Process(input interface{}) ([][]byte, error)
}
