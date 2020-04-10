// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.api;

import com.huaouo.stormy.api.stream.OutputCollector;
import com.huaouo.stormy.api.stream.OutputStreamDeclarer;

public interface ISpout {
    void nextTuple(OutputCollector collector);
    
    void declareOutputStream(OutputStreamDeclarer declarer);
}
