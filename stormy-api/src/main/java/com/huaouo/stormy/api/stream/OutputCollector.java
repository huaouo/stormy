// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.api.stream;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

@Slf4j
public class OutputCollector {

    private Map<String, DynamicSchema> outboundSchemaMap;
    private Map<String, BlockingQueue<byte[]>> outboundQueueMap;

    public OutputCollector(Map<String, DynamicSchema> outboundSchemaMap,
                           Map<String, BlockingQueue<byte[]>> outboundQueueMap) {
        this.outboundSchemaMap = outboundSchemaMap;
        this.outboundQueueMap = outboundQueueMap;
    }

    public void emit(String target, Value... tupleElements) {
        DynamicSchema schema = outboundSchemaMap.get(target);
        if (schema == null) {
            throw new RuntimeException("No such schema: " + target);
        }
        DynamicMessage.Builder msgBuilder = schema.newMessageBuilder("TupleData");
        Descriptors.Descriptor msgDesc = msgBuilder.getDescriptorForType();
        for (Value v : tupleElements) {
            msgBuilder.setField(msgDesc.findFieldByName(v.getName()), v.getValue());
        }
        try {
            outboundQueueMap.get(target).put(msgBuilder.build().toByteArray());
        } catch (InterruptedException e) {
            log.error(e.toString());
        }
    }
}
