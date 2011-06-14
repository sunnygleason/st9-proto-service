#!/bin/sh

java -cp target/dependency/"*":target/"*" -Dhttp.port=7331 \
  -Dst9.storageModule="com.g414.st9.proto.service.store.MySQLKeyValueStorage\$MySQLKeyValueStorageModule" \
  -Djdbc.url=jdbc:mysql://127.0.0.1:3306/thedb \
  -Djdbc.user=root \
  -Djdbc.password="" \
  -Dst9.cacheClass="com.g414.st9.proto.service.cache.RedisKeyValueCache" \
  -Dredis.host=127.0.0.1 \
  com.g414.st9.proto.service.Main

