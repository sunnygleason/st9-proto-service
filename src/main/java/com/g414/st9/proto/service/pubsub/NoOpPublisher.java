package com.g414.st9.proto.service.pubsub;

import java.util.Map;

public class NoOpPublisher implements Publisher {
    public void publish(final String topic, final Map<String, Object> message) {
    }
}
