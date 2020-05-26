// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.workerprocess.acker;

import io.lettuce.core.codec.RedisCodec;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class AckerCodec implements RedisCodec<TopologyTupleId, Integer> {

    @Override
    public TopologyTupleId decodeKey(ByteBuffer bytes) {
        int spoutTupleId = bytes.getInt();

        byte[] topologyNameBytes = new byte[bytes.remaining()];
        bytes.get(topologyNameBytes);
        String topologyName = new String(topologyNameBytes, StandardCharsets.UTF_8);
        return new TopologyTupleId(topologyName, spoutTupleId);
    }

    @Override
    public Integer decodeValue(ByteBuffer bytes) {
        return bytes.getInt();
    }

    @Override
    public ByteBuffer encodeKey(TopologyTupleId key) {
        byte[] topologyNameBytes = key.getTopologyName().getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(4 + topologyNameBytes.length);
        buf.putInt(key.getSpoutTupleId());
        buf.put(topologyNameBytes);
        buf.flip();
        return buf;
    }

    @Override
    public ByteBuffer encodeValue(Integer value) {
        ByteBuffer buf = ByteBuffer.allocate(4);
        buf.putInt(value);
        buf.flip();
        return buf;
    }
}
