// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.example;

import com.huaouo.stormy.stream.*;
import com.huaouo.stormy.topology.Bolt;

public class ExampleBolt implements Bolt {

    @Override
    public void compute(Tuple tuple, OutputController controller) {

    }

    @Override
    public void declareOutputStream(OutputStreamDeclarer declarer) {
        declarer.declare("default",
                new Field(FieldType.INT, "Id"),
                new Field(FieldType.STRING, "Name")
        );
    }
}
