// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.api.stream;

public interface OutputCollector {

    void emit(String target, Value... tupleElements);
}
