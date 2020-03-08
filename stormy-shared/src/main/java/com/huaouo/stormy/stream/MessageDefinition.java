// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

/*
 * Copyright 2015 protobuf-dynamic developers
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huaouo.stormy.stream;

import java.util.HashMap;
import java.util.Map;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;

/**
 * MessageDefinition
 */
public class MessageDefinition {
    // --- public static ---

    public static Builder newBuilder(String msgTypeName) {
        return new Builder(msgTypeName);
    }

    // --- public ---

    public String toString() {
        return mMsgType.toString();
    }

    // --- package ---
    // TODO: changed to package
    public DescriptorProto getMessageType() {
        return mMsgType;
    }

    // --- private ---

    private MessageDefinition(DescriptorProto msgType) {
        mMsgType = msgType;
    }

    private DescriptorProto mMsgType;

    /**
     * MessageDefinition.Builder
     */
    public static class Builder {
        // --- public ---

        public Builder addField(FieldType type, String name) {
            FieldDescriptorProto.Builder fieldBuilder = FieldDescriptorProto.newBuilder();
            fieldBuilder.setLabel(FieldDescriptorProto.Label.LABEL_REQUIRED);
            FieldDescriptorProto.Type primType = sTypeMap.get(type);
            fieldBuilder.setType(primType);
            fieldBuilder.setName(name).setNumber(++messageNumber);
            mMsgTypeBuilder.addField(fieldBuilder.build());
            return this;
        }

        public MessageDefinition build() {
            return new MessageDefinition(mMsgTypeBuilder.build());
        }

        // --- private ---

        private Builder(String msgTypeName) {
            mMsgTypeBuilder = DescriptorProto.newBuilder();
            mMsgTypeBuilder.setName(msgTypeName);
        }

        private int messageNumber = 0;
        private DescriptorProto.Builder mMsgTypeBuilder;
    }

    // --- private static ---

    private static Map<FieldType, FieldDescriptorProto.Type> sTypeMap;

    static {
        sTypeMap = new HashMap<>();
        sTypeMap.put(FieldType.BOOLEAN, FieldDescriptorProto.Type.TYPE_BOOL);
        sTypeMap.put(FieldType.INT, FieldDescriptorProto.Type.TYPE_INT32);
        sTypeMap.put(FieldType.LONG, FieldDescriptorProto.Type.TYPE_INT64);
        sTypeMap.put(FieldType.FLOAT, FieldDescriptorProto.Type.TYPE_FLOAT);
        sTypeMap.put(FieldType.DOUBLE, FieldDescriptorProto.Type.TYPE_DOUBLE);
        sTypeMap.put(FieldType.BYTES, FieldDescriptorProto.Type.TYPE_BYTES);
        sTypeMap.put(FieldType.STRING, FieldDescriptorProto.Type.TYPE_STRING);
    }
}
