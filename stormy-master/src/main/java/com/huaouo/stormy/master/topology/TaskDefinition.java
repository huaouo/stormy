// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.master.topology;

import com.huaouo.stormy.api.topology.EdgeDefinition;
import com.huaouo.stormy.api.topology.NodeDefinition;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Data
public class TaskDefinition {
    private int processNum;
    private int threadsPerProcess;
    private List<String> inboundStreamIds;
    private List<String> outboundStreamIds;

    public TaskDefinition(NodeDefinition node, List<EdgeDefinition> outboundEdges) {
        processNum = node.getProcessNum();
        threadsPerProcess = node.getThreadNumPerProcess();
        inboundStreamIds = new ArrayList<>();
        outboundStreamIds = outboundEdges.stream()
                .map(EdgeDefinition::getStreamId)
                .collect(Collectors.toList());
    }

    public void addInboundStream(String inboundStreamId) {
        inboundStreamIds.add(inboundStreamId);
    }
}
