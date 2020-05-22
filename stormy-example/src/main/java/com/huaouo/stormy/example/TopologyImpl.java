// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.example;

import com.huaouo.stormy.api.ITopology;
import com.huaouo.stormy.api.topology.TopologyDefinition;
import com.huaouo.stormy.api.topology.TopologyException;

public class TopologyImpl implements ITopology {

    @Override
    public TopologyDefinition defineTopology() throws TopologyException {
        return TopologyDefinition.newBuilder()
                .setSpout("spout", SpoutImpl.class, 1, 1)
                .addBolt("intermediateBolt", IntermediateBoltImpl.class, 1, 1)
                .addBolt("bolt", BoltImpl.class, 1, 1)
                .addStream("spout", "intermediateBolt", "myStream")
                .addStream("intermediateBolt", "bolt", "myStream")
                .build();
    }
}
