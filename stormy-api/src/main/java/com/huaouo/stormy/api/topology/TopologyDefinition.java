// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.api.topology;

import com.huaouo.stormy.api.IBolt;
import com.huaouo.stormy.api.ISpout;
import com.huaouo.stormy.api.util.ApiUtil;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class TopologyDefinition {
    private Map<String, NodeDefinition> nodes = new HashMap<>();
    private Map<String, EdgeDefinition> graph = new HashMap<>();

    private TopologyDefinition(Map<String, NodeDefinition> nodes,
                               Map<String, EdgeDefinition> graph) {
        this.nodes = nodes;
        this.graph = graph;
    }

    public static class Builder {
        private String spoutId;
        // boltId || spoutId => NodeDefinition {className, isSpout, processNum, threadNumPerProcess}
        private Map<String, NodeDefinition> nodes = new HashMap<>();
        // sourceId => EdgeDefinition {targetId, streamId}
        private Map<String, EdgeDefinition> graph = new HashMap<>();

        public TopologyDefinition.Builder setSpout(String spoutId, Class<? extends ISpout> spoutClass,
                                                   int processNum, int threadNumPerProcess) {
            ApiUtil.validateId(spoutId);
            if (this.spoutId != null) {
                nodes.remove(this.spoutId);
            }
            this.spoutId = spoutId;
            nodes.put(spoutId, new NodeDefinition(spoutClass.getName(),
                    true, processNum, threadNumPerProcess));
            return this;
        }

        public TopologyDefinition.Builder addBolt(String boltId, Class<? extends IBolt> boltClass,
                                                  int processNum, int threadNumPerProcess) {
            ApiUtil.validateId(boltId);
            if (spoutId != null && spoutId.equals(boltId)) {
                throw new IllegalArgumentException("boltId shouldn't be same as spoutId");
            }
            nodes.put(boltId, new NodeDefinition(boltClass.getName(),
                    false, processNum, threadNumPerProcess));
            return this;
        }

        public TopologyDefinition.Builder addStream(String sourceId, String targetId, String streamId) {
            ApiUtil.validateId(sourceId);
            ApiUtil.validateId(targetId);
            ApiUtil.validateId(streamId);
            graph.put(sourceId, new EdgeDefinition(targetId, sourceId + "-" + streamId));
            return this;
        }

        public TopologyDefinition build() {
            return new TopologyDefinition(nodes, graph);
        }
    }
}
