// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.example;

import com.huaouo.stormy.api.stream.*;
import com.huaouo.stormy.api.ISpout;
import com.huaouo.stormy.example.model.UserInfo;

public class ExampleSpout implements ISpout {

    private UserInfo info = new UserInfo(0, "Zhenhua Yang", "test@gmail.com", "N/A");

    @Override
    public void nextTuple(OutputCollector collector) {
        collector.emit("target1",
                new Value("Id", info.getId()),
                new Value("Name", info.getName()));
        collector.emit("target2",
                new Value("Email", info.getEmail()),
                new Value("Address", info.getAddress()));
    }

    @Override
    public void declareOutputStream(OutputStreamDeclarer declarer) {
        declarer
                .addSchema("target1",
                        new Field("Id", FieldType.INT),
                        new Field("Name", FieldType.STRING))
                .addSchema("target2",
                        new Field("Email", FieldType.STRING),
                        new Field("Address", FieldType.STRING));
    }
}
