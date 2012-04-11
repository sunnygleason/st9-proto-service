package com.g414.st9.proto.service.pubsub;

import java.util.Map;

public interface Publisher {
    public void publish(String topic, Map<String, Object> message);
}
