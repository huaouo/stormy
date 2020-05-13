// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.api;

import com.huaouo.stormy.api.stream.OutputStreamDeclarer;

public interface IOperator {

    void declareOutputStream(OutputStreamDeclarer declarer);
}
