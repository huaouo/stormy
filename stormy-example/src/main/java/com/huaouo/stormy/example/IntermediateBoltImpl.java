// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.example;

import com.huaouo.stormy.api.IBolt;
import com.huaouo.stormy.api.stream.*;

public class IntermediateBoltImpl implements IBolt {
    @Override
    public void compute(Tuple tuple, OutputCollector collector) {
        collector.emit("myStream", new Value("Name", tuple.getStringByName("Name")));
    }

    @Override
    public void declareOutputStream(OutputStreamDeclarer declarer) {
        declarer.addSchema("myStream", new Field("Name", FieldType.STRING));
    }
}
