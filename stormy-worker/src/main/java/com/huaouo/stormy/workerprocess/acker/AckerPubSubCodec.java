// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.workerprocess.acker;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

import java.nio.ByteBuffer;

public class AckerPubSubCodec implements RedisCodec<String, TopologyTupleId> {

    private final AckerCodec ackerCodec = new AckerCodec();
    private final StringCodec stringCodec = new StringCodec();

    @Override
    public String decodeKey(ByteBuffer bytes) {
        return stringCodec.decodeKey(bytes);
    }

    @Override
    public TopologyTupleId decodeValue(ByteBuffer bytes) {
        return ackerCodec.decodeKey(bytes);
    }

    @Override
    public ByteBuffer encodeKey(String key) {
        return stringCodec.encodeKey(key);
    }

    @Override
    public ByteBuffer encodeValue(TopologyTupleId value) {
        return ackerCodec.encodeKey(value);
    }
}
