// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.master.topology;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
public class ComputationGraph {
    // nodeId => TaskDefinition
    private Map<String, TaskDefinition> tasks;
    // Refer to TopologyLoader#getAssignOrder
    private List<String> assignOrder;

    public int getTotalThreads() {
        return tasks.values().stream()
                .mapToInt(t -> t.getProcessNum() * t.getThreadsPerProcess())
                .sum();
    }
}
