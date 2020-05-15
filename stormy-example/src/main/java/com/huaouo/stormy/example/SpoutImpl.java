// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.example;

import com.huaouo.stormy.api.ISpout;
import com.huaouo.stormy.api.stream.*;

public class SpoutImpl implements ISpout {

    private String[] names = {"Zhenhua Yang", "Andy Park", "Bob Gates"};
    private int nameIndex = 0;

    @Override
    public void nextTuple(OutputCollector collector) {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignored) {
        }
        collector.emit("myStream", new Value("Name", names[nameIndex++ % names.length]));
    }

    @Override
    public void declareOutputStream(OutputStreamDeclarer declarer) {
        declarer.addSchema("myStream", new Field("Name", FieldType.STRING));
    }
}
