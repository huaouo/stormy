// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.api;

import com.huaouo.stormy.api.stream.OutputCollector;

public interface ISpout extends IOperator {

    void nextTuple(OutputCollector collector);
}
