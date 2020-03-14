package com.huaouo.stormy.provider;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;

public class RedisConnection {

    private RedisClient redisClient;
    private StatefulRedisConnection<String, String> redisConnection;

    public RedisConnection(RedisClient redisClient,
                           StatefulRedisConnection<String, String> redisConnection) {
        this.redisClient = redisClient;
        this.redisConnection = redisConnection;
    }

    public RedisCommands<String, String> sync() {
        return redisConnection.sync();
    }

    public RedisAsyncCommands<String, String> async() {
        return redisConnection.async();
    }

    public void close() {
        redisConnection.close();
        redisClient.shutdown();
    }
}
