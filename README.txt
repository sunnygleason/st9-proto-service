
To build this, run:

$ mvn package
$ mvn dependency:copy-dependencies

To run this, run:

$ java -cp target/dependency/"*":target/st9-proto-service-0.1.0.jar com.g414.st9.proto.service.Main

To use alternate storage engines (In-Memory, SQLite, MySQL):

$ java -cp target/dependency/"*":target/st9-proto-service-0.1.0.jar -Dst9.storageModule="com.g414.st9.proto.service.store.InMemoryKeyValueStorage\$InMemoryKeyValueStorageModule" com.g414.st9.proto.service.Main
$ java -cp target/dependency/"*":target/st9-proto-service-0.1.0.jar -Dst9.storageModule="com.g414.st9.proto.service.store.SqliteKeyValueStorage\$SqliteKeyValueStorageModule" com.g414.st9.proto.service.Main
$ java -cp target/dependency/"*":target/st9-proto-service-0.1.0.jar -Dst9.storageModule="com.g414.st9.proto.service.store.MySQLKeyValueStorage\$MySQLKeyValueStorageModule" com.g414.st9.proto.service.Main

NOTE: to use the MySQL storage engine, there must be a database called 'thedb' on the local machine.
(This will soon be a configurable option). Likewise, SQLite writes to "thedb.db".


A cook's tour of the KV api:

$ curl -v -v -X POST --data '{"isAwesome":false}' -H "Content-Type: application/json" -H "Accept: application/json" "http://localhost:8080/1.0/e/foo" 
$ curl -v -v -X GET "http://localhost:8080/1.0/e/foo:1"
$ curl -v -v -X PUT --data "{\"foo\":true}" -H "Content-Type: application/json" "http://localhost:8080/1.0/e/foo:1"
$ curl -v -v -X GET "http://localhost:8080/1.0/e/foo:1"
$ curl -v -v -X DELETE "http://localhost:8080/1.0/e/foo:1"
$ curl -v -v -X GET "http://localhost:8080/1.0/e/foo:1"

A cook's tour of the query api:

$ curl -v -v -X GET "http://localhost:8080/1.0/i/foo.index1?q=isAwesome+eq+true"
$ curl -v -v -X GET "http://localhost:8080/1.0/i/foo.index1?q=isAwesome+eq+false"
$ curl -v -v -X GET "http://localhost:8080/1.0/i/foo.index2?q=id+lt+\"foo:2\"+and+isAwesome+eq+true"


That's all for now.
