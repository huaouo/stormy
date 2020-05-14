// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.workerprocess.topology;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.huaouo.stormy.api.stream.DynamicSchema;
import com.huaouo.stormy.api.stream.OutputCollector;
import com.huaouo.stormy.api.stream.Value;
import com.huaouo.stormy.workerprocess.thread.ComputedOutput;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

@Slf4j
public class OutputCollectorImpl implements OutputCollector {

    private Map<String, DynamicSchema> outboundSchemaMap;
    private BlockingQueue<ComputedOutput> outboundQueue;

    public OutputCollectorImpl(Map<String, DynamicSchema> outboundSchemaMap,
                               BlockingQueue<ComputedOutput> outboundQueue) {
        this.outboundSchemaMap = outboundSchemaMap;
        this.outboundQueue = outboundQueue;
    }

    @Override
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
            outboundQueue.put(new ComputedOutput(target, msgBuilder.build().toByteArray()));
        } catch (InterruptedException e) {
            log.error(e.toString());
        }
    }
}
