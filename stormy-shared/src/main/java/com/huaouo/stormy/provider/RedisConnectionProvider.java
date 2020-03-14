// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.provider;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Provider;
import java.util.Properties;

@Slf4j
public class RedisConnectionProvider implements Provider<RedisConnection> {

    @Inject
    private Properties appProp;

    @Override
    public RedisConnection get() {
        String redisURI = appProp.getProperty("stormy.redis.uri");
        if (redisURI == null) {
            log.error("Missing 'stormy.redis.uri' in 'application.properties'");
            System.exit(-1);
        }
        RedisClient redisClient = RedisClient.create(redisURI);
        StatefulRedisConnection<String, String> redisConnection = redisClient.connect();
        return new RedisConnection(redisClient, redisConnection);
    }
}
