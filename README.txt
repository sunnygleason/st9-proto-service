
To build this, run:

$ mvn package
$ mvn dependency:copy-dependencies

To run this, run:

$ java -cp target/dependency/"*":target/"*" com.g414.st9.proto.service.Main

To use alternate storage engines (In-Memory, SQLite, MySQL):

$ java -cp target/dependency/"*":target/"*" -Dst9.storageModule="com.g414.st9.proto.service.store.InMemoryKeyValueStorage\$InMemoryKeyValueStorageModule" com.g414.st9.proto.service.Main
$ java -cp target/dependency/"*":target/"*" -Dst9.storageModule="com.g414.st9.proto.service.store.SqliteKeyValueStorage\$SqliteKeyValueStorageModule" com.g414.st9.proto.service.Main
$ java -cp target/dependency/"*":target/"*" -Dst9.storageModule="com.g414.st9.proto.service.store.MySQLKeyValueStorage\$MySQLKeyValueStorageModule" com.g414.st9.proto.service.Main

NOTE: to use the MySQL storage engine, there must be a database called 'thedb' on the local machine.
(This will soon be a configurable option). Likewise, SQLite writes to "thedb.db".


A cook's tour of the Schema api:

$ curl -v -v -X POST --data "{\"attributes\":[{\"name\":\"x\",\"type\":\"I32\"},{\"name\":\"y\",\"type\":\"I32\"}],\"indexes\":[{\"name\":\"xy\",\"cols\":[{\"name\":\"x\",\"sort\":\"ASC\"},{\"name\":\"y\",\"sort\":\"ASC\"},{\"name\":\"id\",\"sort\":\"ASC\"}]}]}" -H "Content-Type: application/json" -H "Accept: application/json" "http://localhost:8080/1.0/s/point"
$ curl -v -v -X GET "http://localhost:8080/1.0/s/point"
$ curl -v -v -X POST --data "{\"x\":1,\"y\":-1}" -H "Content-Type: application/json" -H "Accept: application/json" "http://localhost:8080/1.0/e/point"
$ curl -v -v -X POST --data "{\"x\":1,\"y\":true}" -H "Content-Type: application/json" -H "Accept: application/json" "http://localhost:8080/1.0/e/point"
$ curl -v -v -X DELETE "http://localhost:8080/1.0/s/point"
$ curl -v -v -X POST --data "{\"x\":1,\"y\":true}" -H "Content-Type: application/json" -H "Accept: application/json" "http://localhost:8080/1.0/e/point"

Query stuff using the (beta) query api:

$ curl -v -v -X GET "http://localhost:8080/1.0/i/point.xy?q=x+eq+1"
$ curl -v -v -X GET "http://localhost:8080/1.0/i/point.xy?q=x+ne+-1+and+y+ne+1"

A cook's tour of the KV api:

$ curl -v -v -X POST --data '{"isAwesome":false}' -H "Content-Type: application/json" -H "Accept: application/json" "http://localhost:8080/1.0/e/foo" 
$ curl -v -v -X GET "http://localhost:8080/1.0/e/foo:1"
$ curl -v -v -X PUT --data "{\"foo\":true}" -H "Content-Type: application/json" "http://localhost:8080/1.0/e/foo:1"
$ curl -v -v -X GET "http://localhost:8080/1.0/e/foo:1"
$ curl -v -v -X DELETE "http://localhost:8080/1.0/e/foo:1"
$ curl -v -v -X GET "http://localhost:8080/1.0/e/foo:1"

Advanced schema and query stuff:

$ curl -v -v -X POST --data "{\"attributes\":[{\"name\":\"msg\",\"type\":\"UTF8_SMALLSTRING\"},{\"name\":\"hotness\",\"type\":\"ENUM\",\"values\":[\"COOL\",\"HOT\"]}],\"indexes\":[{\"name\":\"hotmsg\",\"cols\":[{\"name\":\"hotness\",\"sort\":\"ASC\"},{\"name\":\"msg\",\"sort\":\"ASC\"},{\"name\":\"id\",\"sort\":\"ASC\"}]}]}" -H "Content-Type: application/json" -H "Accept: application/json" "http://localhost:8080/1.0/s/message"
$ curl -v -v -X POST -H "Content-Type: application/json" -H "Accept: application/json" --data "{\"msg\":\"hello world\",\"hotness\":\"COOL\"}" "http://localhost:8080/1.0/e/message"
$ curl -v -v -X POST -H "Content-Type: application/json" -H "Accept: application/json" --data "{\"msg\":\"fly like a G6\",\"hotness\":\"HOT\"}" "http://localhost:8080/1.0/e/message"
$ curl -v -v -X GET "http://localhost:8080/1.0/i2/message.hotmsg?q=hotness+eq+\"COOL\""
$ curl -v -v -X GET "http://localhost:8080/1.0/i2/message.hotmsg?q=hotness+eq+\"HOT\""
$ curl -v -v -X GET "http://localhost:8080/1.0/i2/message.hotmsg?q=hotness+eq+\"FOO\""
$ curl -v -v -X GET "http://localhost:8080/1.0/i2/message.hotmsg?q=hotness+eq+\"HOT\"+and+msg+lt+\"he\""
$ curl -v -v -X GET "http://localhost:8080/1.0/i2/message.hotmsg?q=hotness+eq+\"HOT\"+and+msg+gt+\"he\""

Advanced counters usage:

$ curl -v -v -X POST --data @src/test/resources/schema20.json.txt -H "Content-Type: application/json" -H "Accept: application/json" "http://localhost:8080/1.0/s/awesome"
$ curl -v -v -X POST --data "{\"target\":\"foo:1\",\"isAwesome\":true,\"hotness\":\"COOL\",\"year\":1970}" -H "Content-Type: application/json" -H "Accept: application/json" "http://localhost:8080/1.0/e/awesome"
$ curl -v -v -X POST --data "{\"target\":\"foo:2\",\"isAwesome\":false,\"hotness\":\"TEH_HOTNESS\",\"year\":1980}" -H "Content-Type: application/json" -H "Accept: application/json" "http://localhost:8080/1.0/e/awesome" 
$ curl "http://localhost:8080/1.0/c/awesome.byTarget"
$ curl "http://localhost:8080/1.0/c/awesome.byTarget/foo:1"
$ curl "http://localhost:8080/1.0/c/awesome.byTarget/foo:2"
$ curl "http://localhost:8080/1.0/c/awesome.byAwesome"
$ curl "http://localhost:8080/1.0/c/awesome.byTargetHotnessYear"
$ curl "http://localhost:8080/1.0/c/awesome.byTargetHotnessYear/@foo:ce4ad6a1cd6293d9/TEH_HOTNESS"
$ curl "http://localhost:8080/1.0/c/awesome.byTargetHotnessYear/@foo:ce4ad6a1cd6293d9/TEH_HOTNESS/1980"

That's all for now.

