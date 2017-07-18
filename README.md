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
