// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.example;

import com.huaouo.stormy.api.stream.*;
import com.huaouo.stormy.api.IBolt;

public class ExampleBaseInfoBolt implements IBolt {

    @Override
    public void compute(Tuple tuple, String sourceId, OutputCollector collector) {
        System.out.print("Id: " + tuple.getIntByName("Id"));
        System.out.println("Name: " + tuple.getStringByName("Name"));
    }

    @Override
    public void declareOutputStream(OutputStreamDeclarer declarer) {
        declarer.addSchema("toOutput",
                new Field("Id", FieldType.STRING),
                new Field("Name", FieldType.STRING));
    }
}
