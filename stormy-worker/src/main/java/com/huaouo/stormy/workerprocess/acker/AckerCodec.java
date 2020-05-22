// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.workerprocess.acker;

import io.lettuce.core.codec.RedisCodec;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class AckerCodec implements RedisCodec<TopologyTupleId, Integer> {

    @Override
    public TopologyTupleId decodeKey(ByteBuffer bytes) {
        int spoutTupleId = decodeInt(bytes);

        byte[] topologyNameBytes = new byte[bytes.remaining()];
        bytes.get(topologyNameBytes);
        String topologyName = new String(topologyNameBytes, StandardCharsets.UTF_8);
        return new TopologyTupleId(topologyName, spoutTupleId);
    }

    @Override
    public Integer decodeValue(ByteBuffer bytes) {
        return decodeInt(bytes);
    }

    public int decodeInt(ByteBuffer bytes) {
        int ret = 0;
        int offset = 0;
        for (int i = 0; i < 4; ++i) {
            ret |= bytes.get() << offset;
            offset += 8;
        }
        return ret;
    }

    @Override
    public ByteBuffer encodeKey(TopologyTupleId key) {
        byte[] topologyNameBytes = key.getTopologyName().getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(4 + topologyNameBytes.length);
        encodeInt(buf, key.getSpoutTupleId());
        buf.put(topologyNameBytes);
        return buf;
    }

    @Override
    public ByteBuffer encodeValue(Integer value) {
        ByteBuffer buf = ByteBuffer.allocate(4);
        encodeInt(buf, value);
        return buf;
    }

    public void encodeInt(ByteBuffer buf, int value) {
        for (int i = 0; i < 4; ++i) {
            buf.put((byte) (value & 0xFF));
            value >>= 8;
        }
    }
}
