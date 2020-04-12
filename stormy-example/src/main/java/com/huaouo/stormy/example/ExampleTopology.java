// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.example;

import com.huaouo.stormy.api.ITopology;
import com.huaouo.stormy.api.topology.TopologyDefinition;
import com.huaouo.stormy.api.topology.TopologyException;

public class ExampleTopology implements ITopology {

    @Override
    public TopologyDefinition defineTopology() throws TopologyException {
        return new TopologyDefinition.Builder()
                .setSpout("spout", ExampleSpout.class, 1, 1)
                .addBolt("baseBolt", ExampleBaseInfoBolt.class, 2, 2)
                .addBolt("extendBolt", ExampleExtendInfoBolt.class, 2, 2)
                .addBolt("outputBolt", ExampleOutputBolt.class, 1, 1)
                .addStream("spout", "baseBolt", "target1")
                .addStream("spout", "extendBolt", "target2")
                .addStream("baseBolt", "outputBolt", "toOutput")
                .addStream("extendBolt", "outputBolt", "toOutput")
                .build();
    }
}
