package com.g414.st9.proto.service.pubsub;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Implementation of publisher to facilitate testing.
 */
public class NoOpRecordingPublisher implements Publisher {
    private final List<Map<String, Object>> values = new ArrayList<Map<String, Object>>();

    @Override
    public void publish(String topic, Map<String, Object> message) {
        values.add(message);
    }

    public void clear() {
        values.clear();
    }

    public List<Map<String, Object>> getValues() {
        return Collections.unmodifiableList(values);
    }
}