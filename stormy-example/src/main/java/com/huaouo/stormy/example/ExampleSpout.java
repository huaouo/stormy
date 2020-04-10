// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.example;

import com.huaouo.stormy.api.stream.Field;
import com.huaouo.stormy.api.stream.FieldType;
import com.huaouo.stormy.api.stream.OutputCollector;
import com.huaouo.stormy.api.stream.OutputStreamDeclarer;
import com.huaouo.stormy.api.ISpout;
import com.huaouo.stormy.example.model.UserInfo;

public class ExampleSpout implements ISpout {

    private UserInfo info = new UserInfo(0, "Zhenhua Yang", "test@gmail.com", "N/A");

    @Override
    public void nextTuple(OutputCollector collector) {
        collector.emit("target1", info.getId(), info.getName());
        collector.emit("target2", info.getEmail(), info.getAddress());
    }

    @Override
    public void declareOutputStream(OutputStreamDeclarer declarer) {
        declarer.addSchema(
                "target1",
                new Field(FieldType.INT, "Id"),
                new Field(FieldType.STRING, "Name")
        );
        declarer.addSchema(
                "target2",
                new Field(FieldType.STRING, "Email"),
                new Field(FieldType.STRING, "Address")
        );
    }
}
