// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.example;

import com.huaouo.stormy.api.IBolt;
import com.huaouo.stormy.api.stream.OutputCollector;
import com.huaouo.stormy.api.stream.OutputStreamDeclarer;
import com.huaouo.stormy.api.stream.Tuple;

public class ExampleOutputBolt implements IBolt {

    @Override
    public void compute(Tuple tuple, String sourceId, OutputCollector controller) {
        if ("baseBolt".equals(sourceId)) {
            System.out.println("Id: " + tuple.getIntByName("Id"));
            System.out.println("Name: " + tuple.getStringByName("Name"));
        } else { // "extendBolt".equals(sourceId)
            System.out.println("Email: " + tuple.getStringByName("Email"));
            System.out.println("Address: " + tuple.getStringByName("Address"));
        }
    }

    @Override
    public void declareOutputStream(OutputStreamDeclarer declarer) {

    }
}
