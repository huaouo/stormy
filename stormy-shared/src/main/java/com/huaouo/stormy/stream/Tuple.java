// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.stream;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;

public class Tuple {

    private DynamicMessage message;
    private Descriptor messageDescriptor;

    public Tuple(DynamicMessage message, Descriptor messageDescriptor) {
        this.message = message;
        this.messageDescriptor = messageDescriptor;
    }

    public boolean getBooleanByName(String fieldName) {
        return (boolean) getFieldByName(fieldName);
    }

    public int getIntByName(String fieldName) {
        return (int) getFieldByName(fieldName);
    }

    public long getLongByName(String fieldName) {
        return (long) getFieldByName(fieldName);
    }

    public float getFloatByName(String fieldName) {
        return (float) getFieldByName(fieldName);
    }

    public double getDoubleByName(String fieldName) {
        return (double) getFieldByName(fieldName);
    }

    public byte[] getBytesByName(String fieldName) {
        return (byte[]) getFieldByName(fieldName);
    }

    public String getStringByName(String fieldName) {
        return (String) getFieldByName(fieldName);
    }

    private Object getFieldByName(String fieldName) {
        return message.getField(messageDescriptor.findFieldByName(fieldName));
    }
}
