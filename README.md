## zen ##

# Problematics #

Evaluate DGraph for Geo fencing.

Given a Kafka topic on which user id + location pairs are published, publish
back on another topic a serie of `fence` messages including the user id and
the name of the fence the user is in.

The messages must be defined using protobuf, code in Go.

The current solution we use at Zenly is custom made and can hold up to several
thousand pair per seconds. We want to know whether DGraph could be used to
replace our custom solution, how we can scale it and how much it costs.

https://en.wikipedia.org/wiki/Geo-fence
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
  docker run -it -p 127.0.0.1:8080:8080 -p 127.0.0.1:9080:9080 -v ~/dgraph:/dgraph --name dgraph dgraph/dgraph dgraph --bindall=true
  ```

# Installation #

* Init DB
  ```
  cd dbinitscript
  go build
  ./dbinitscript --nb-fences=10000 --max-lines=3 --long-delta=7.2 --lat-delta=3.4 --out-file=fences
  ```

* Generate proto files
  ```
  cd ../producingmessageconsumer
  protoc --go_out=$GOPATH/src *.proto
  cd ../objects
  protoc --go_out=. *.proto --proto_path=../producingmessageconsumer --proto_path=.
  ```

* Publish user id + location pairs
  ```
  cd ../userpospublisher
  go build
  ./userpospublisher --seed=42 --nb-msgs=1000 --topic=userpos
  ```

* Publish fences
  ```
  cd ../fencepublisher
  go build
  ./fencepublisher --topic-user-loc=userpos --topic-user-fence=fence
  ```