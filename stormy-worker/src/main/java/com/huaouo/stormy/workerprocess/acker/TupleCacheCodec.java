// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.workerprocess.acker;

import com.huaouo.stormy.workerprocess.thread.ComputedOutput;
import io.lettuce.core.codec.RedisCodec;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class TupleCacheCodec implements RedisCodec<TopologyTupleId, ComputedOutput> {

    private final AckerCodec ackerCodec = new AckerCodec();

    @Override
    public TopologyTupleId decodeKey(ByteBuffer bytes) {
        return ackerCodec.decodeKey(bytes);
    }

    @Override
    public ComputedOutput decodeValue(ByteBuffer bytes) {
        int streamIdLen = ackerCodec.decodeInt(bytes);
        byte[] streamIdBytes = new byte[streamIdLen];
        bytes.get(streamIdBytes);
        byte[] dataBytes = new byte[bytes.remaining()];
        bytes.get(dataBytes);
        return new ComputedOutput(new String(streamIdBytes, StandardCharsets.UTF_8), dataBytes);
    }

    @Override
    public ByteBuffer encodeKey(TopologyTupleId key) {
        return ackerCodec.encodeKey(key);
    }

    @Override
    public ByteBuffer encodeValue(ComputedOutput value) {
        byte[] streamIdBytes = value.getStreamId().getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(4 + streamIdBytes.length + value.getBytes().length);
        ackerCodec.encodeInt(buf, streamIdBytes.length);
        buf.put(streamIdBytes);
        buf.put(value.getBytes());
        return buf;
    }
}
