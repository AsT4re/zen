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

# Configuration #

Kafka need to run locally and Dgraph has to run depending on the desired configuration (1 or multiple nodes).

* Kafka:
  ```
  docker pull spotify/kafka
  docker run -p 2181:2181 -p 9092:9092 --env ADVERTISED_PORT=9092 --env ADVERTISED_HOST=localhost spotify/kafka
  ```

* DGraph (v0.8.3):

  This is an example for a simple dgraph configuration with one node. In order to test with more nodes, check out https://docs.dgraph.io/deploy.
  ```
  docker pull dgraph/dgraph:v0.8.3
  docker run -it -p 8080:8080 -p 9080:9080 -v ~/dgraph:/dgraph --name dgraph dgraph/dgraph:v0.8.3 dgraphzero -w zw
  docker exec -it dgraph dgraph --bindall=true --memory_mb 8192 -peer 127.0.0.1:8888
  ```

# Evaluation #

* Evaluate Dgraph. Launch the desired dgraph cluster configuration and execute the 'evaldgraph' binary. It will dump a file with statistics for this configuration.
  For example, for a dgraph cluster with 2 nodes:
  ```
  go get github.com/AsT4re/zen/evaldgraph
  evaldgraph --topic-user-loc="UserLoc" --topic-user-fence="UserFence" --dg-host-and-port=<ipNodeDgraph>:9080 --dg-host-and-port=<ipNode2Dgraph>:9080 --stats-file=dg-2-nodes
  ```
* Generate graphs based on statistics files
  For example if you have run the evaldgraph script two times on a configuration of 1 and then 2 nodes, you can load both files and generate graphs based on it:
  ```
  go get github.com/AsT4re/zen/generategraphs
  generategraphs --input=../evaldgraph/dg-1-node --input=../evaldgraph/dg-2-nodes
  ```