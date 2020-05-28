// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.example;

import com.huaouo.stormy.api.ITopology;
import com.huaouo.stormy.api.topology.TopologyDefinition;
import com.huaouo.stormy.api.topology.TopologyException;

public class SimpleTopology implements ITopology {

    @Override
    public TopologyDefinition defineTopology() throws TopologyException {
        return TopologyDefinition.newBuilder()
                .setSpout("spout", SimpleSpout.class, 1, 1)
                .addBolt("intermediateBolt", SimpleIntermediateBolt.class, 1, 1)
                .addBolt("bolt", SimpleBolt.class, 1, 1)
                .addStream("spout", "intermediateBolt", "myStream")
                .addStream("intermediateBolt", "bolt", "myStream")
                .build();
    }
}
