package com.g414.st9.proto.service.pubsub;

import static com.g414.st9.proto.service.helper.RedisTemplates.withJedisCallback;

import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.g414.st9.proto.service.helper.EncodingHelper;
import com.g414.st9.proto.service.helper.RedisTemplates.JedisCallback;

public class RedisPublisher implements Publisher {
    private final JedisPool jedisPool;

    public RedisPublisher() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setTestOnBorrow(true);

        this.jedisPool = new JedisPool(config, System.getProperty("redis.host",
                "localhost"), Integer.parseInt(System.getProperty("redis.port",
                "6379")));
    }

    public void publish(final String topic, final Map<String, Object> message) {
        withJedisCallback(jedisPool, new JedisCallback<Void>() {
            @Override
            public Void withJedis(Jedis jedis) {
                jedis.publish(topic.getBytes(),
                        EncodingHelper.convertToSmileLzf(message));

                return null;
            }
        });
    }
}
