package pmc

type MessageHandler interface {
	Process(input []byte) ([][]byte, error)
}
