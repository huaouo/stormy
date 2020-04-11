// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.api.stream;

import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.huaouo.stormy.api.util.ApiUtil;

import java.util.HashMap;
import java.util.Map;

public class OutputStreamDeclarer {
    private String streamIdPrefix;
    private Map<String, DynamicSchema> outputStreamSchemas = new HashMap<>();

    // streamIdPrefix should be spoutId or boltId
    public OutputStreamDeclarer(String streamIdPrefix) {
        this.streamIdPrefix = streamIdPrefix;
    }

    public OutputStreamDeclarer addSchema(String streamId, Field... fields) {
        ApiUtil.validateId(streamId);
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
        return this;
    }

    public Map<String, DynamicSchema> getOutputStreamSchemas() {
        return outputStreamSchemas;
    }
}
