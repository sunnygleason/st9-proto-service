
To build this, run:

$ mvn package
$ mvn dependency:copy-dependencies

To run this, run:

$ java -cp target/dependency/"*":target/st9-proto-service-0.1.0.jar com.g414.st9.proto.service.Main

A cook's tour of the KV api:

$ curl -v -v -X POST --data '{"isAwesome":false}' -H "Content-Type: application/json" -H "Accept: application/json" "http://localhost:8080/1.0/e/foo" 
$ curl -v -v -X GET "http://localhost:8080/1.0/e/foo:1"
$ curl -v -v -X PUT --data "{\"foo\":true}" -H "Content-Type: application/json" "http://localhost:8080/1.0/e/foo:1"
$ curl -v -v -X GET "http://localhost:8080/1.0/e/foo:1"
$ curl -v -v -X DELETE "http://localhost:8080/1.0/e/foo:1"
$ curl -v -v -X GET "http://localhost:8080/1.0/e/foo:1"

That's all for now.
