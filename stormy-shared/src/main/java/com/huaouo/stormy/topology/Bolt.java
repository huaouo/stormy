package com.huaouo.stormy.topology;

import com.huaouo.stormy.stream.OutputController;
import com.huaouo.stormy.stream.OutputStreamDeclarer;
import com.huaouo.stormy.stream.Tuple;

public interface Bolt {
    void compute(Tuple tuple, OutputController controller);

    void declareOutputStream(OutputStreamDeclarer declarer);
}
