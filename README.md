# zen

Problematics:

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

Requirements:
* Protobuf compiler

Install hints:

Kafka:
* docker pull spotify/kafka
* docker run -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=`docker-machine ip \`docker-machine active\`` --env ADVERTISED_PORT=9092 spotify/kafka

DGraph:
* docker pull dgraph/dgraph
* docker run -it -p 127.0.0.1:8080:8080 -p 127.0.0.1:9080:9080 -v ~/dgraph:/dgraph --name dgraph dgraph/dgraph dgraph --bindall=true

make install