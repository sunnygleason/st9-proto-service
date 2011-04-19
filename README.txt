
To build this, run:

$ mvn package
$ mvn dependency:copy-dependencies

To run this, run:

$ java -jar target/st9-proto-service.jar

To use alternate storage engines (In-Memory, SQLite, MySQL):

$ java -Dst9.storageModule="com.g414.st9.proto.service.store.InMemoryKeyValueStorage\$InMemoryKeyValueStorageModule" -jar target/st9-proto-service.jar
$ java -Dst9.storageModule="com.g414.st9.proto.service.store.SqliteKeyValueStorage\$SqliteKeyValueStorageModule" -jar target/st9-proto-service.jar
$ java -Dst9.storageModule="com.g414.st9.proto.service.store.MySQLKeyValueStorage\$MySQLKeyValueStorageModule" -jar target/st9-proto-service.jar

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


That's all for now.

