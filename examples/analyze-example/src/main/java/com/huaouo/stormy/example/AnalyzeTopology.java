// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.example;

import com.huaouo.stormy.api.ITopology;
import com.huaouo.stormy.api.topology.TopologyDefinition;
import com.huaouo.stormy.api.topology.TopologyException;

public class AnalyzeTopology implements ITopology {

    @Override
    public TopologyDefinition defineTopology() throws TopologyException {
        return TopologyDefinition.newBuilder()
                .setSpout("logSpout", LogSpout.class, 3, 1)
                .addBolt("splitBolt", SplitBolt.class, 2, 2)
                .addBolt("referBolt", ReferBolt.class, 1, 1)
                .addBolt("uaBolt", UaBolt.class, 1, 1)
                .addBolt("urlBolt", UrlBolt.class, 1, 1)
                .addStream("logSpout", "splitBolt", "logStream")
                .addStream("splitBolt", "referBolt", "referStream")
                .addStream("splitBolt", "uaBolt", "uaStream")
                .addStream("splitBolt", "urlBolt", "urlStream")
                .build();
    }
}
