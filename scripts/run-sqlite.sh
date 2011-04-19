#!/bin/sh

mkdir -p logs

java -Dst9.storageModule="com.g414.st9.proto.service.store.SqliteKeyValueStorage\$SqliteKeyValueStorageModule" -jar target/st9-proto-service.jar

