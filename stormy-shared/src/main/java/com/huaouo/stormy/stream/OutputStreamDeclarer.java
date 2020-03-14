// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.stream;

import com.google.protobuf.Descriptors.DescriptorValidationException;

import java.util.HashMap;
import java.util.Map;

//@Slf4j TODO
public class OutputStreamDeclarer {

    private Map<String, DynamicSchema> outputStreamSchemas = new HashMap<>();

    public OutputStreamDeclarer declare(String stringId, Field... fields) {
        MessageDefinition.Builder msgDefBuilder = MessageDefinition.newBuilder("TupleData");
        for (Field f : fields) {
            msgDefBuilder.addField(f.fieldType, f.fieldName);
        }
        DynamicSchema schema = null;
        try {
            schema = DynamicSchema.newBuilder()
                    .addMessageDefinition(msgDefBuilder.build())
                    .build();
        } catch (DescriptorValidationException e) {
            // 预期不会抛出异常
//            log.error(e.getDescription());
        }
        outputStreamSchemas.put(stringId, schema);
        return this;
    }

    // TODO: 可见性
    Map<String, DynamicSchema> getOutputStreamSchemas() {
        return outputStreamSchemas;
    }
}
