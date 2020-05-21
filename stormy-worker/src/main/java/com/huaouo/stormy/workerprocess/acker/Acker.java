// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.workerprocess.acker;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.huaouo.stormy.api.IBolt;
import com.huaouo.stormy.api.stream.OutputCollector;
import com.huaouo.stormy.api.stream.OutputStreamDeclarer;
import com.huaouo.stormy.api.stream.Tuple;
import com.huaouo.stormy.shared.GuiceModule;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;

import java.util.Properties;

public class Acker implements IBolt {

    private final RedisAsyncCommands<String, String> redisCommands;

    public Acker() {
        Injector injector = Guice.createInjector(new GuiceModule());
        Properties appProps = injector.getInstance(Properties.class);
        String redisUriStr = appProps.getProperty("stormy.redis.uri");
        RedisURI redisUri = RedisURI.create(redisUriStr);

        RedisClient redisClient = RedisClient.create(redisUri);
        StatefulRedisConnection<String, String> conn = redisClient.connect();
        redisCommands = conn.async();
    }

    @Override
    public void compute(Tuple tuple, OutputCollector collector) {
        String topologyTupleId = tuple.getStringByName("topologyTupleId");
        int traceId = tuple.getIntByName("traceId");
        redisCommands.multi();
        redisCommands.set("tmpTraceId", Integer.toString(traceId));
        redisCommands.bitopXor(topologyTupleId, topologyTupleId, "tmpTraceId");
        // TODO: change ttl dynamically or find a suitable value or add to config file
        redisCommands.expire(topologyTupleId, 10);
        redisCommands.exec();
    }

    @Override
    public void declareOutputStream(OutputStreamDeclarer declarer) {
    }
}
