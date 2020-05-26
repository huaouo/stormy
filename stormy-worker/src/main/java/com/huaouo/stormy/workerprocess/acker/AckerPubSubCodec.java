// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.workerprocess.acker;

import io.lettuce.core.codec.RedisCodec;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class AckerPubSubCodec implements RedisCodec<String, TopologyTupleId> {

    private final AckerCodec ackerCodec = new AckerCodec();

    @Override
    public String decodeKey(ByteBuffer bytes) {
        byte[] stringBytes = new byte[bytes.remaining()];
        bytes.get(stringBytes);
        return new String(stringBytes, StandardCharsets.UTF_8);
    }

    @Override
    public TopologyTupleId decodeValue(ByteBuffer bytes) {
        return ackerCodec.decodeKey(bytes);
    }

    @Override
    public ByteBuffer encodeKey(String key) {
        byte[] stringBytes = key.getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.wrap(stringBytes);
    }

    @Override
    public ByteBuffer encodeValue(TopologyTupleId value) {
        return ackerCodec.encodeKey(value);
    }
}
