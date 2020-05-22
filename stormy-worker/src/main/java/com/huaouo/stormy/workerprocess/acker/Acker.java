// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.workerprocess.acker;

import com.huaouo.stormy.api.IBolt;
import com.huaouo.stormy.api.stream.OutputCollector;
import com.huaouo.stormy.api.stream.OutputStreamDeclarer;
import com.huaouo.stormy.api.stream.Tuple;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Acker implements IBolt {

    private final RedisAsyncCommands<TopologyTupleId, Integer> redisCommands;
    private final TopologyTupleId tmpKey = new TopologyTupleId("tmpKey", 0);
    private String deleteIfZeroDigest;

    public Acker() {
        String redisUriStr = System.getProperty("stormy.redis.trace_uri");
        RedisURI redisUri = RedisURI.create(redisUriStr);

        RedisClient redisClient = RedisClient.create(redisUri);
        StatefulRedisConnection<TopologyTupleId, Integer> conn = redisClient.connect(new AckerCodec());
        redisCommands = conn.async();
        try {
            deleteIfZeroDigest = redisCommands.scriptLoad("local r = redis.call('get', KEYS[1]); " +
                    "if(r == '\\0\\0\\0\\0') then " +
                    "return redis.call('del', KEYS[1]); end").get();
        } catch (Throwable t) {
            log.error("Failed to load deleteIfZero scripts: " + t.toString());
            System.exit(-1);
        }
    }

    @Override
    public void compute(Tuple tuple, OutputCollector collector) {
        String topologyName = tuple.getStringByName("_topologyName");
        int spoutTupleId = tuple.getIntByName("_spoutTupleId");
        int traceId = tuple.getIntByName("_traceId");
        TopologyTupleId topologyTupleId = new TopologyTupleId(topologyName, spoutTupleId);

        redisCommands.multi();
        redisCommands.set(tmpKey, traceId);
        redisCommands.setnx(topologyTupleId, 0);
        redisCommands.bitopXor(topologyTupleId, topologyTupleId, tmpKey);
        // TODO: change ttl dynamically or find a suitable value or add to config file
        redisCommands.expire(topologyTupleId, 10);
        redisCommands.evalsha(deleteIfZeroDigest, ScriptOutputType.INTEGER, topologyTupleId);
        redisCommands.exec();
    }

    @Override
    public void declareOutputStream(OutputStreamDeclarer declarer) {
    }
}
