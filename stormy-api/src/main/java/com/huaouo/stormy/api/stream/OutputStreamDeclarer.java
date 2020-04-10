// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.api.stream;

import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.huaouo.stormy.api.util.SharedUtil;

import java.util.HashMap;
import java.util.Map;

public class OutputStreamDeclarer {
    private String streamIdPrefix;
    private Map<String, DynamicSchema> outputStreamSchemas = new HashMap<>();

    public OutputStreamDeclarer(String streamIdPrefix) {
        this.streamIdPrefix = streamIdPrefix;
    }

    public void addSchema(String streamId, Field... fields) {
        SharedUtil.validateId(streamId);
        MessageDefinition.Builder msgDefBuilder = MessageDefinition.newBuilder("TupleData");
        for (Field f : fields) {
            msgDefBuilder.addField(f.fieldType, f.fieldName);
        }
        DynamicSchema schema = null;
        try {
            schema = DynamicSchema.newBuilder()
                    .addMessageDefinition(msgDefBuilder.build())
                    .build();
        } catch (DescriptorValidationException ignored) {
        }
        outputStreamSchemas.put(streamIdPrefix + "-" + streamId, schema);
    }

    public Map<String, DynamicSchema> getOutputStreamSchemas() {
        return outputStreamSchemas;
    }
}
