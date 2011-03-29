#!/bin/sh

java -cp target/dependency/"*":target/"*" -Dst9.storageModule="com.g414.st9.proto.service.store.SqliteKeyValueStorage\$SqliteKeyValueStorageModule" com.g414.st9.proto.service.Main

