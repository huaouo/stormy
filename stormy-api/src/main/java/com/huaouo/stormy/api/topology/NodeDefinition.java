// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.api.topology;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class NodeDefinition {
    private String className;
    private boolean spout;
    private int processNum;
    private int threadNumPerProcess;
}
