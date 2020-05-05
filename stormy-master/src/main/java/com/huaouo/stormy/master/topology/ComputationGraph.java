// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.master.topology;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.Map;
import java.util.Set;

@Data
@AllArgsConstructor
public class ComputationGraph {
    private String spoutId;
    // nodeId => TaskDefinition
    private Map<String, TaskDefinition> tasks;
    // nodeId#x => Set<nodeId#x>, ensure a spoutId#0 key exists
    private Map<TaskInstance, Set<TaskInstance>> graph;
}
