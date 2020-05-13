// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.workerprocess.thread;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TransmitTupleClientThread implements Runnable {

    private Map<String, BlockingQueue<byte[]>> outboundQueueMap;

    public void initWithOutbounds(Set<String> outbounds) {
        Map<String, LinkedBlockingQueue<byte[]>> m = new HashMap<>();
        for (String o : outbounds) {
            outboundQueueMap.put(o, new LinkedBlockingQueue<>());
        }
        outboundQueueMap = Collections.unmodifiableMap(m);
    }

    public Map<String, BlockingQueue<byte[]>> getOutboundQueueMap() {
        return outboundQueueMap;
    }

    @Override
    public void run() {
        if (outboundQueueMap == null) {
            throw new RuntimeException("Not initialized");
        }
    }
}
