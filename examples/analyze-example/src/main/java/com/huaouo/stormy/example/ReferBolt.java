// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.example;

import com.huaouo.stormy.api.IBolt;
import com.huaouo.stormy.api.stream.OutputCollector;
import com.huaouo.stormy.api.stream.OutputStreamDeclarer;
import com.huaouo.stormy.api.stream.Tuple;

public class ReferBolt implements IBolt {

    @Override
    public void compute(Tuple tuple, OutputCollector collector) {
        String refer = tuple.getStringByName("refer");
    }

    @Override
    public void declareOutputStream(OutputStreamDeclarer declarer) {

    }
}
