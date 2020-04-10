// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.api;

import com.huaouo.stormy.api.stream.OutputCollector;
import com.huaouo.stormy.api.stream.OutputStreamDeclarer;
import com.huaouo.stormy.api.stream.Tuple;

public interface IBolt {
    void compute(Tuple tuple, OutputCollector controller);

    void declareOutputStream(OutputStreamDeclarer declarer);
}
