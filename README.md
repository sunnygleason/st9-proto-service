# Introduction

ST9 is a nimble and nifty data store that is designed for scalability.

* _Nimble_ : because you only lock down the attributes of the schema that you *have* to
* _Nifty_ : because it introduces some features that SQL databases don't have -- is not your average SQL data store
* _Scalable_ : because ST9 is built from scalable primitives (key-value store, separate secondary indexes, and constant-time counters)

Some other noteworthy features of ST9:

* REST API : designed to be *super* easy to work with any language or even the command line (i.e. Ruby, node.js, curl)
* Document-Oriented : stored entities may contain complex JSON structure (nested arrays and objects)
* Multi-Get : retrieving several entities at one time without breaking a sweat!
* Caching : can use a near cache (Memcached or Redis) to offload key-value lookups and support multi-get
* Secondary Indexes : define indexes (think: database tables with _just_ the data you need) to support range queries and unique constraints
* Counters: define counters (think: select count(*) where ... group by X,Y,Z) to support data aggregation with minimal (constant time) overhead per update
* Binary Encoding: under the hood, ST9 stores data in SMILE (binary JSON) format to minimize data storage requirements
* Compression: under the hood, ST9 stores data with LZF compression to minimize data storage requirements even further


# Building ST9

To build st9, run:

$ ./scripts/build.sh


# Running ST9

To run st9, run:

$ ./scripts/run-sqlite.sh

(There is also a mysql version for power users.)


# Using ST9


## Schema API - the /s/ endpoint

A cook's tour of the Schema API, used to define entity types (think: tables but not tables):

\# creates a schema for a new entity type

$ curl -v -v -X POST --binary-data @src/test/resources/schema07.json.txt -H "Content-Type: application/json" -H "Accept: application/json" "http://localhost:7331/1.0/s/point"

\# retrieves a schema for the given entity type

$ curl -v -v -X GET "http://localhost:7331/1.0/s/point"

\# updates a schema for the given entity type (including migrating counters and indexes as necessary)

$ curl -v -v -X PUT --binary-data @src/test/resources/schema07b.json.txt -H "Content-Type: application/json" -H "Accept: application/json" "http://localhost:7331/1.0/s/point"

\# removes a data type (caution: do not use)

$ curl -v -v -X DELETE "http://localhost:7331/1.0/s/point"


## Entity API - the /e/ endpoint

Creating, retrieving, updating and deleting entities (think: rows but more like documents):

\# creating a new entity

$ curl -v -v -X POST --data "{\"x\":1,\"y\":-1}" -H "Content-Type: application/json" -H "Accept: application/json" "http://localhost:7331/1.0/e/point"

\# example of GET a newly-created entity

$ curl -v -v -X GET "http://localhost:7331/1.0/e/@point:5eae81437e481933"

\# example of POST invalid data (type mismatch)

$ curl -v -v -X POST --data "{\"x\":1,\"y\":true}" -H "Content-Type: application/json" -H "Accept: application/json" "http://localhost:7331/1.0/e/point"


# Under construction - this documentation still being updated

## Index API - the /i/ endpoint

Query stuff using the query api:

$ curl -v -v -X GET "http://localhost:7331/1.0/i/point.xy?q=x+eq+1"
$ curl -v -v -X GET "http://localhost:7331/1.0/i/point.xy?q=x+ne+-1+and+y+ne+1"

A cook's tour of the KV api:

$ curl -v -v -X POST --data '{"isAwesome":false}' -H "Content-Type: application/json" -H "Accept: application/json" "http://localhost:7331/1.0/e/foo" 
$ curl -v -v -X GET "http://localhost:7331/1.0/e/foo:1"
$ curl -v -v -X PUT --data "{\"foo\":true}" -H "Content-Type: application/json" "http://localhost:7331/1.0/e/foo:1"
$ curl -v -v -X GET "http://localhost:7331/1.0/e/foo:1"
$ curl -v -v -X DELETE "http://localhost:7331/1.0/e/foo:1"
$ curl -v -v -X GET "http://localhost:7331/1.0/e/foo:1"

Advanced schema and query stuff:

$ curl -v -v -X POST --data "{\"attributes\":[{\"name\":\"msg\",\"type\":\"UTF8_SMALLSTRING\"},{\"name\":\"hotness\",\"type\":\"ENUM\",\"values\":[\"COOL\",\"HOT\"]}],\"indexes\":[{\"name\":\"hotmsg\",\"cols\":[{\"name\":\"hotness\",\"sort\":\"ASC\"},{\"name\":\"msg\",\"sort\":\"ASC\"},{\"name\":\"id\",\"sort\":\"ASC\"}]}]}" -H "Content-Type: application/json" -H "Accept: application/json" "http://localhost:7331/1.0/s/message"
$ curl -v -v -X POST -H "Content-Type: application/json" -H "Accept: application/json" --data "{\"msg\":\"hello world\",\"hotness\":\"COOL\"}" "http://localhost:7331/1.0/e/message"
$ curl -v -v -X POST -H "Content-Type: application/json" -H "Accept: application/json" --data "{\"msg\":\"fly like a G6\",\"hotness\":\"HOT\"}" "http://localhost:7331/1.0/e/message"
$ curl -v -v -X GET "http://localhost:7331/1.0/i2/message.hotmsg?q=hotness+eq+\"COOL\""
$ curl -v -v -X GET "http://localhost:7331/1.0/i2/message.hotmsg?q=hotness+eq+\"HOT\""
$ curl -v -v -X GET "http://localhost:7331/1.0/i2/message.hotmsg?q=hotness+eq+\"FOO\""
$ curl -v -v -X GET "http://localhost:7331/1.0/i2/message.hotmsg?q=hotness+eq+\"HOT\"+and+msg+lt+\"he\""
$ curl -v -v -X GET "http://localhost:7331/1.0/i2/message.hotmsg?q=hotness+eq+\"HOT\"+and+msg+gt+\"he\""

## Counters API - the /c/ endpoint

Advanced counters usage:

$ curl -v -v -X POST --data @src/test/resources/schema20.json.txt -H "Content-Type: application/json" -H "Accept: application/json" "http://localhost:7331/1.0/s/awesome"
$ curl -v -v -X POST --data "{\"target\":\"foo:1\",\"isAwesome\":true,\"hotness\":\"COOL\",\"year\":1970}" -H "Content-Type: application/json" -H "Accept: application/json" "http://localhost:7331/1.0/e/awesome"
$ curl -v -v -X POST --data "{\"target\":\"foo:2\",\"isAwesome\":false,\"hotness\":\"TEH_HOTNESS\",\"year\":1980}" -H "Content-Type: application/json" -H "Accept: application/json" "http://localhost:7331/1.0/e/awesome" 
$ curl "http://localhost:7331/1.0/c/awesome.byTarget"
$ curl "http://localhost:7331/1.0/c/awesome.byTarget/foo:1"
$ curl "http://localhost:7331/1.0/c/awesome.byTarget/foo:2"
$ curl "http://localhost:7331/1.0/c/awesome.byAwesome"
$ curl "http://localhost:7331/1.0/c/awesome.byTargetHotnessYear"
$ curl "http://localhost:7331/1.0/c/awesome.byTargetHotnessYear/@foo:ce4ad6a1cd6293d9/TEH_HOTNESS"
$ curl "http://localhost:7331/1.0/c/awesome.byTargetHotnessYear/@foo:ce4ad6a1cd6293d9/TEH_HOTNESS/1980"

That's all for now.

