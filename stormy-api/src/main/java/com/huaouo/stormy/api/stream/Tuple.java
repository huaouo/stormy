// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.api.stream;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;

// TODO: getXXXByName in this class will throw NullPointerException
//       if fieldName doesn't exist in tuple, fix it later
public class Tuple {

    private final DynamicMessage message;
    private final Descriptor messageDescriptor;

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
