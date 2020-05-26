// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.workerprocess.acker;

import com.huaouo.stormy.workerprocess.thread.ComputedOutput;
import io.lettuce.core.codec.RedisCodec;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class TupleCacheCodec implements RedisCodec<TopologyTupleId, CachedComputedOutput> {

    private final AckerCodec ackerCodec = new AckerCodec();

    @Override
    public TopologyTupleId decodeKey(ByteBuffer bytes) {
        return ackerCodec.decodeKey(bytes);
    }

    @Override
    public CachedComputedOutput decodeValue(ByteBuffer bytes) {
        int initTraceId = bytes.getInt();
        int streamIdLen = bytes.getInt();
        byte[] streamIdBytes = new byte[streamIdLen];
        bytes.get(streamIdBytes);
        byte[] dataBytes = new byte[bytes.remaining()];
        bytes.get(dataBytes);
        ComputedOutput out = new ComputedOutput(new String(streamIdBytes, StandardCharsets.UTF_8), dataBytes);
        return new CachedComputedOutput(initTraceId, out);
    }

    @Override
    public ByteBuffer encodeKey(TopologyTupleId key) {
        return ackerCodec.encodeKey(key);
    }

    @Override
    public ByteBuffer encodeValue(CachedComputedOutput value) {
        ComputedOutput out = value.getComputedOutput();
        byte[] streamIdBytes = out.getStreamId().getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(8 + streamIdBytes.length + out.getBytes().length);
        buf.putInt(value.getInitTraceId());
        buf.putInt(streamIdBytes.length);
        buf.put(streamIdBytes);
        buf.put(out.getBytes());
        buf.flip();
        return buf;
    }
}
