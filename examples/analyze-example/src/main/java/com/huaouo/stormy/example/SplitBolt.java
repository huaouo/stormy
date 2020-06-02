// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.example;

import com.huaouo.stormy.api.IBolt;
import com.huaouo.stormy.api.stream.*;

public class SplitBolt implements IBolt {

    @Override
    public void compute(Tuple tuple, OutputCollector collector) {
        String log = tuple.getStringByName("log");
        String url = log.split(" ")[6];
        collector.emit("urlStream", new Value("url", url));

        String ua = log.split("\"")[5];
        collector.emit("uaStream", new Value("ua", ua));
    }

    @Override
    public void declareOutputStream(OutputStreamDeclarer declarer) {
        declarer.addSchema("urlStream", new Field("url", FieldType.STRING))
                .addSchema("uaStream", new Field("ua", FieldType.STRING));
    }
}
