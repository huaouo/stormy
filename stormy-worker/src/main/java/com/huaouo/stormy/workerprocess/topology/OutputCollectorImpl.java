// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.workerprocess.topology;

import com.google.protobuf.Descriptors.Descriptor;
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

    private final Map<String, DynamicSchema> outboundSchemaMap;
    private final BlockingQueue<ComputedOutput> outboundQueue;
    private final BeforeEmitCallback beforeEmit;
    private String streamIdPrefix = "";

    public OutputCollectorImpl(Map<String, DynamicSchema> outboundSchemaMap,
                               BlockingQueue<ComputedOutput> outboundQueue,
                               BeforeEmitCallback beforeEmit) {
        this.outboundSchemaMap = outboundSchemaMap;
        if (!outboundSchemaMap.isEmpty()) {
            String randomStreamId = outboundSchemaMap.keySet().iterator().next();
            String[] slicedStreamId = randomStreamId.split("-");
            streamIdPrefix = slicedStreamId[0] + "-" + slicedStreamId[1] + "-";
        }
        this.outboundQueue = outboundQueue;
        this.beforeEmit = beforeEmit;
    }

    @Override
    public void emit(String targetStream, Value... tupleElements) {
        targetStream = streamIdPrefix + targetStream;
        DynamicSchema schema = outboundSchemaMap.get(targetStream);
        if (schema == null) {
            throw new RuntimeException("No such schema: " + targetStream);
        }
        DynamicMessage.Builder msgBuilder = schema.newMessageBuilder("TupleData");
        Descriptor msgDesc = msgBuilder.getDescriptorForType();
        for (Value v : tupleElements) {
            msgBuilder.setField(msgDesc.findFieldByName(v.getName()), v.getValue());
        }
        ComputedOutput output = beforeEmit.accept(msgBuilder, msgDesc, targetStream);
        try {
            outboundQueue.put(output);
        } catch (InterruptedException e) {
            log.error(e.toString());
        }
    }
}
