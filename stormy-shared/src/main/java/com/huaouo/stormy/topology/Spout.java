package com.huaouo.stormy.topology;

import com.huaouo.stormy.stream.OutputController;
import com.huaouo.stormy.stream.OutputStreamDeclarer;

public interface Spout {
    void nextTuple(OutputController controller);

    void declareOutputStream(OutputStreamDeclarer declarer);
}
