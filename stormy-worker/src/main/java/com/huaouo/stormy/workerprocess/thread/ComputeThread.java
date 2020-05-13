// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.workerprocess.thread;

import com.huaouo.stormy.api.IBolt;
import com.huaouo.stormy.api.IOperator;
import com.huaouo.stormy.api.ISpout;
import com.huaouo.stormy.api.stream.DynamicSchema;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class ComputeThread implements Runnable {

    private String taskName;
    private IOperator operator;
    private DynamicSchema inboundSchema;
    private BlockingQueue<byte[]> inboundQueue;
    private Map<String, BlockingQueue<byte[]>> outboundQueueMap;

    public ComputeThread(String taskName,
                         Class<? extends IOperator> operatorClass,
                         DynamicSchema inboundSchema,
                         BlockingQueue<byte[]> inboundQueue,
                         Map<String, BlockingQueue<byte[]>> outboundQueueMap)
            throws IllegalAccessException, InstantiationException {
        this.taskName = taskName;
        this.operator = operatorClass.newInstance();
        this.inboundSchema = inboundSchema;
        this.inboundQueue = inboundQueue;
        this.outboundQueueMap = outboundQueueMap;
    }

    @Override
    public void run() {
        if (operator instanceof ISpout) {
            spoutLoop();
        } else { // IBolt
            boltLoop();
        }
    }

    private void spoutLoop() {
        ISpout spout = (ISpout) operator;
        while (true) {

        }
    }

    private void boltLoop() {
        IBolt bolt = (IBolt) operator;
        while (true) {

        }
    }
}
