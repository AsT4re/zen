default: vet

vet:
	go vet ./...

install:
	go get github.com/Shopify/sarama
	go get -u github.com/golang/protobuf/protoc-gen-go
	protoc -I=$(GOPATH)/src/zen/protobuf --go_out=$(GOPATH)/src/zen/protobuf $(GOPATH)/src/zen/protobuf/UserFence.proto
	protoc -I=$(GOPATH)/src/zen/protobuf --go_out=$(GOPATH)/src/zen/protobuf $(GOPATH)/src/zen/protobuf/UserPosition.proto
