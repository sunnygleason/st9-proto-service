# Introduction

ST9 is a nimble and nifty data store that is designed for scalability.

* _Nimble_ : because you only lock down the attributes of the schema that you *have* to
* _Nifty_ : because it introduces some features that SQL databases don't have -- is not your average SQL data store
* _Scalable_ : because ST9 is built from scalable primitives (key-value store, separate secondary indexes, and constant-time counters)

Some other noteworthy features of ST9:

* REST API : designed to be *super* easy to work with any language or even the command line (i.e. Ruby, node.js, curl)
* Developer-Friendly : provides Sqlite3 and H2 data stores for easy development, as well as the super-scalable MySQL-based store
* Document-Oriented : stored entities may contain complex JSON structure (nested arrays and objects)
* Multi-Get : retrieving several entities at one time without breaking a sweat!
* 2-Query Model: secondary index queries return identifiers (primary keys), allowing client to decide which entities to load
* Caching : can use a near cache (Memcached or Redis) to offload key-value lookups and support multi-get
* Secondary Indexes : define indexes (think: database tables with _just_ the data you need) to support range queries and unique constraints
* Counters: schemas may define counters (think: select count(*) where ... group by X,Y,Z) to support data aggregation with minimal (constant time) overhead per update
* Binary Encoding: under the hood, ST9 stores data in SMILE (binary JSON) format to minimize data storage requirements
* Compression: under the hood, ST9 stores data with LZF compression to minimize data storage requirements even further


# Building ST9

To build st9, run:

$ ./scripts/build.sh


# Running ST9

To run st9, run:

$ ./scripts/run-sqlite.sh

(There is also a mysql version for power users, and an H2 (embedded in-memory) store for serious development.)


# Using ST9


## Schema API - the /s/ endpoint

A cook's tour of the Schema API, used to define entity types (think: tables but not tables):

\# creates a schema for a new entity type

$ curl -v -v -X POST --data-binary @src/test/resources/schema07.json.txt -H "Content-Type: application/json" -H "Accept: application/json" "http://localhost:7331/1.0/s/point"

\# retrieves a schema for the given entity type

$ curl -v -v -X GET "http://localhost:7331/1.0/s/point"

\# updates a schema for the given entity type (including migrating counters and indexes as necessary)

$ curl -v -v -X PUT --data-binary @src/test/resources/schema07b.json.txt -H "Content-Type: application/json" -H "Accept: application/json" "http://localhost:7331/1.0/s/point"

\# removes a data type (caution: do not use)

$ curl -v -v -X DELETE "http://localhost:7331/1.0/s/point"


## Entity API - the /e/ endpoint

Creating, retrieving, updating and deleting entities (think: rows but more like documents):

\# using POST to create a new entity

$ curl -v -v -X POST --data "{\"x\":1,\"y\":-1}" -H "Content-Type: application/json" -H "Accept: application/json" "http://localhost:7331/1.0/e/point"

\# example of POST invalid data (type mismatch)

$ curl -v -v -X POST --data "{\"x\":1,\"y\":true}" -H "Content-Type: application/json" -H "Accept: application/json" "http://localhost:7331/1.0/e/point"

\# example of GET a newly-created entity

$ curl -v -v -X GET "http://localhost:7331/1.0/e/@point:5eae81437e481933"

\# updating an entity with PUT

$ curl -v -v -X PUT --data "{\"x\":7,\"y\":-3}" -H "Content-Type: application/json" -H "Accept: application/json" "http://localhost:7331/1.0/e/@point:5eae81437e481933"

\# using DELETE to remove an entity

$ curl -v -v -X DELETE "http://localhost:7331/1.0/e/@point:5eae81437e481933"


## Index API - the /i/ endpoint

Querying entities using secondary (non-primary key) indexes.

\# Query point entities using the 'xy' index where 'x' equals 1:

$ curl -v -v -X GET "http://localhost:7331/1.0/i/point.xy?q=x+eq+1"

\# Query point entities using the 'xy' index where 'x' not equal -1 and where 'y' not equal 1

$ curl -v -v -X GET "http://localhost:7331/1.0/i/point.xy?q=x+ne+-1+and+y+ne+1"

\# Query point entities using the 'xy' index where 'x' is in the list (1, 2, 3) and where 'y' ne 1

$ curl -v -v -X GET "http://localhost:7331/1.0/i/point.xy?q=x+in+(1,2,3)+and+y+ne+1"

A query is a conjunction (list AND'ed together) of query terms.

Query term format (binary operators): _field_ _op_ _value_ 

Query term format (in operator): _field_ in ( _value1_ , _value2_ )

Operators:

* _eq_ : equals operator
* _ne_ : not equals operator (try to avoid, requires full index scan)
* _gt_ : greater than operator
* _ge_ : greater than or equals operator
* _lt_ : less than operator
* _le_ : less than or equals operator

Possible value literals:

* Integers: 1, -1, 0, ...
* String literals: "some text between double quotes"
* Boolean literals: true, false
* Null literal: null


Advanced schema and query stuff:

\# defining a schema (entity type) 

$ curl -v -v -X POST --data "{\"attributes\":[{\"name\":\"msg\",\"type\":\"UTF8_SMALLSTRING\"},{\"name\":\"hotness\",\"type\":\"ENUM\",\"values\":[\"COOL\",\"HOT\"]}],\"indexes\":[{\"name\":\"hotmsg\",\"cols\":[{\"name\":\"hotness\",\"sort\":\"ASC\"},{\"name\":\"msg\",\"sort\":\"ASC\"},{\"name\":\"id\",\"sort\":\"ASC\"}]}]}" -H "Content-Type: application/json" -H "Accept: application/json" "http://localhost:7331/1.0/s/message"

\# creating a couple entities

$ curl -v -v -X POST -H "Content-Type: application/json" -H "Accept: application/json" --data "{\"msg\":\"hello world\",\"hotness\":\"COOL\"}" "http://localhost:7331/1.0/e/message"

$ curl -v -v -X POST -H "Content-Type: application/json" -H "Accept: application/json" --data "{\"msg\":\"fly like a G6\",\"hotness\":\"HOT\"}" "http://localhost:7331/1.0/e/message"

\# querying entities by enum value

$ curl -v -v -X GET "http://localhost:7331/1.0/i2/message.hotmsg?q=hotness+eq+\"COOL\""

$ curl -v -v -X GET "http://localhost:7331/1.0/i2/message.hotmsg?q=hotness+eq+\"HOT\""

$ curl -v -v -X GET "http://localhost:7331/1.0/i2/message.hotmsg?q=hotness+eq+\"FOO\""

\# querying entities by enum value and string range queries

$ curl -v -v -X GET "http://localhost:7331/1.0/i2/message.hotmsg?q=hotness+eq+\"HOT\"+and+msg+lt+\"he\""

$ curl -v -v -X GET "http://localhost:7331/1.0/i2/message.hotmsg?q=hotness+eq+\"HOT\"+and+msg+gt+\"he\""


## Counters API - the /c/ endpoint

Counters are pre-computed aggregations (think: GROUP BY, but maintained during entity updates) that allow efficient counts of entities.

\# Define an entity with counters

$ curl -v -v -X POST --data @src/test/resources/schema20.json.txt -H "Content-Type: application/json" -H "Accept: application/json" "http://localhost:7331/1.0/s/awesome"

\# Post a couple entities

$ curl -v -v -X POST --data "{\"target\":\"foo:1\",\"isAwesome\":true,\"hotness\":\"COOL\",\"year\":1970}" -H "Content-Type: application/json" -H "Accept: application/json" "http://localhost:7331/1.0/e/awesome"

$ curl -v -v -X POST --data "{\"target\":\"foo:2\",\"isAwesome\":false,\"hotness\":\"TEH_HOTNESS\",\"year\":1980}" -H "Content-Type: application/json" -H "Accept: application/json" "http://localhost:7331/1.0/e/awesome" 

\# Retrieve the first page of counts for the 'byTarget' counter

$ curl "http://localhost:7331/1.0/c/awesome.byTarget"

\# Retrieve the specific counters corresponding to 'foo:1' and 'foo:2'

$ curl "http://localhost:7331/1.0/c/awesome.byTarget/foo:1"

$ curl "http://localhost:7331/1.0/c/awesome.byTarget/foo:2"

\# Counters may be queried hierarchically by providing column values in left to right order

$ curl "http://localhost:7331/1.0/c/awesome.byAwesome"

$ curl "http://localhost:7331/1.0/c/awesome.byTargetHotnessYear"

$ curl "http://localhost:7331/1.0/c/awesome.byTargetHotnessYear/@foo:ce4ad6a1cd6293d9/TEH_HOTNESS"

$ curl "http://localhost:7331/1.0/c/awesome.byTargetHotnessYear/@foo:ce4ad6a1cd6293d9/TEH_HOTNESS/1980"


## Administrative Endpoints

\# ping the data store

$ curl -v -v -X GET "http://localhost:7331/ping"

\# nuke (delete) all data in the data store, including schema

$ curl -v -v -X POST "http://localhost:7331/1.0/nuke"

\# nuke (delete) all data in the data store, including schema

$ curl -v -v -X POST "http://localhost:7331/1.0/nuke?preserveSchemas=true"

\# export all data in the data store in one-json-object-per-line format

$ curl -v -v -X GET "http://localhost:7331/1.0/x"

\# import data into the data store, deleting all previous data

$ curl -v -v -X POST -H "Content-Type: application/json" --data-binary @the-file.json.txt "http://localhost:7331/1.0/x"


## System Configuration Properties

Properties that are currently supported:

* _http.port_ : port to listen on (default 8080)
* _st9.storageModule_ : module name of storage module (default "com.g414.st9.proto.service.store.SqliteKeyValueStorage\$SqliteKeyValueStorageModule", "com.g414.st9.proto.service.store.MySQLKeyValueStorage\$MySQLKeyValueStorageModule")
* _st9.cacheClass_ : class name of cache module (default none, "com.g414.st9.proto.service.cache.RedisKeyValueCache",  "com.g414.st9.proto.service.cache.MemcachedKeyValueCache")
* _log.dir_ : log directory (default "logs")
* _jdbc.url_ : url for jdbc data store (default "jdbc:sqlite:thedb.db")
* _jdbc.user_ : username for jdbc data store
* _jdbc.password_ : password for jdbc data store
* _redis.host_ : hostname of redis service (default "localhost")
* _redis.port_ : port of redis service (default "")
* _memcached.host_ : hostname and port of memcached service in "host:port" form (default "localhost:11211")
* _nuke.allowed_ : whether nukes are allowed on the data store (default "false")
* _strict.type.creation_ : whether types must be created via the schema endpoint before use (default "true")


That's all for now.

