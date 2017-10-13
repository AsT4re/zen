## zen ##

# Problematics #

Evaluation of DGraph for Geo fencing.

Given a Kafka topic on which user id + location pairs are published, we want
to match fences in DB that contain this location and then publish these fences
on another topic.

This solution uses go as language, protobuf for message definition and Kafka as
message broker. We want to analyze throughput for this specific problem using DGraph
as a database for storing geo index of polygons.

https://dgraph.io/

# Requirements #

* Protobuf compiler (protoc 3.3.0 min)
* Kafka:
  ```
  docker pull spotify/kafka
  docker run -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=`docker-machine ip \`docker-machine active\`` --env ADVERTISED_PORT=9092 spotify/kafka
  ```

* DGraph:
  ```
  docker pull dgraph/dgraph
  docker run -it -p 8080:8080 -p 9080:9080 -v ~/dgraph:/dgraph --name dgraph dgraph/dgraph:latest dgraphzero -w zw
  docker exec -it dgraph dgraph --bindall=true --memory_mb 8192 -peer 127.0.0.1:8888
  ```

# Installation #

* Generate proto files
  ```
  cd libs/producingmessageconsumer
  protoc --go_out=$GOPATH/src *.proto
  cd ../objects
  protoc --go_out=. *.proto --proto_path=../producingmessageconsumer --proto_path=.
  ```
* Evaluate Dgraph. Launch the desired dgraph cluster configuration and execute the 'evaldgraph' binary. It will dump a file with statistics.
  ```
  cd ../../evaldgraph
  go get
  go build
  ./evaldgraph --topic-user-loc="UserLoc" --topic-user-fence="UserFence" --dg-host-and-port=<ipNodeDgraph>:9080 --dg-host-and-port=<ipNode2Dgraph>:9080 --stats-file=dg-2-nodes
  ```
* Generate graphs based on statistics files
  ```
  cd ../generategraphs
  go get
  go build
  ./generategraphs --input=../evaldgraph/dg-1-node --input=../evaldgraph/dg-2-nodes
  ```