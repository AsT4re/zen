package pmc

type Message struct {
	Value interface{}
	Metas *MessageMetadatas
}

type MessageHandler interface {
	// For getting message metadatas (retry infos...)
	Unmarshal(input []byte) (*Message, error)
	// For retry, we need to marshall back message with new metadatas
	Marshal(output *Message) ([]byte, error)
	// Process message without metadatas and give back a serie of messages as output
	Process(input interface{}) ([][]byte, error)
}
