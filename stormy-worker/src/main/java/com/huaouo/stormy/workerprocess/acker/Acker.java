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
import io.micrometer.core.instrument.Counter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class Acker implements IBolt {

    private final RedisAsyncCommands<TopologyTupleId, Integer> redisCommands;
    private final TopologyTupleId tmpKey = new TopologyTupleId("tmpKey", 0);
    private String getTupleAckTimeScript;
    private Counter tupleCount;
    private Counter latencySum;

    public Acker() {
        String redisUriStr = System.getProperty("stormy.redis.trace_uri");
        RedisURI redisUri = RedisURI.create(redisUriStr);

        RedisClient redisClient = RedisClient.create(redisUri);
        StatefulRedisConnection<TopologyTupleId, Integer> conn = redisClient.connect(new AckerCodec());
        redisCommands = conn.async();
        try {
            getTupleAckTimeScript = redisCommands.scriptLoad(
                    "local r = redis.call('get', KEYS[1]);\n" +
                            "if(r == '\\0\\0\\0\\0') then\n" +
                            "  redis.call('del', KEYS[1]);\n" +
                            "  local low = redis.call('get', KEYS[2]);\n" +
                            "  redis.call('del', KEYS[2]);\n" +
                            "  local high = redis.call('get', KEYS[3]);\n" +
                            "  redis.call('del', KEYS[3]);\n" +
                            "  local low_bytes = {string.byte(low, 1, -1)};\n" +
                            "  local high_bytes = {string.byte(high, 1, -1)};\n" +
                            "  local len_high_bytes = #high_bytes;\n" +
                            "  for i=1,#low_bytes do\n" +
                            "    high_bytes[len_high_bytes + i] = low_bytes[i];\n" +
                            "  end\n" +
                            "  return high_bytes;\n" +
                            "end\n" +
                            "return {};\n").get();
        } catch (Throwable t) {
            log.error("Failed to load getTupleAckTimeScript scripts: " + t.toString());
            System.exit(-1);
        }
    }

    public void setCounters(Counter tupleCount, Counter latencySum) {
        this.tupleCount = tupleCount;
        this.latencySum = latencySum;
    }

    @Override
    public void compute(Tuple tuple, OutputCollector collector) {
        String topologyName = tuple.getStringByName("_topologyName");
        int spoutTupleId = tuple.getIntByName("_spoutTupleId");
        int traceId = tuple.getIntByName("_traceId");
        TopologyTupleId topologyTupleId = new TopologyTupleId(topologyName, spoutTupleId);
        long time = System.currentTimeMillis();
        TopologyTupleId timeHighId = new TopologyTupleId("timeh-" + topologyName, spoutTupleId);
        TopologyTupleId timeLowId = new TopologyTupleId("timel-" + topologyName, spoutTupleId);

        synchronized (this) {
            redisCommands.multi();
            redisCommands.set(tmpKey, traceId);
            redisCommands.setnx(topologyTupleId, 0);
            redisCommands.setnx(timeLowId, (int) time);
            redisCommands.setnx(timeHighId, (int) (time >> 32));
            redisCommands.bitopXor(topologyTupleId, topologyTupleId, tmpKey);
            // TODO: change ttl dynamically or find a suitable value or add to config file
            redisCommands.expire(topologyTupleId, 10);
            redisCommands.evalsha(getTupleAckTimeScript, ScriptOutputType.MULTI, topologyTupleId, timeLowId, timeHighId)
                    .thenAcceptAsync(result -> {
                        @SuppressWarnings("unchecked")
                        List<Long> byteList = (List<Long>) result;
                        if (byteList.size() == 0) {
                            return;
                        }
                        long beginTime = 0;
                        for (Long b : byteList) {
                            beginTime <<= 8;
                            beginTime |= b & 0xFFL;
                        }
                        updateTupleLatency(System.currentTimeMillis() - beginTime);
                    });
            redisCommands.exec();
        }
    }

    void updateTupleLatency(long milliseconds) {
        tupleCount.increment();
        latencySum.increment(milliseconds);
    }

    @Override
    public void declareOutputStream(OutputStreamDeclarer declarer) {
    }
}
