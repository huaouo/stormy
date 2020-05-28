// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.example;

import com.huaouo.stormy.api.ISpout;
import com.huaouo.stormy.api.stream.*;

public class SimpleSpout implements ISpout {

    private int x = 0;

    @Override
    public void nextTuple(OutputCollector collector) {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignored) {
        }
        collector.emit("myStream", new Value("Id", x++));
    }

    @Override
    public void declareOutputStream(OutputStreamDeclarer declarer) {
        declarer.addSchema("myStream", new Field("Id", FieldType.INT));
    }
}
