// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.workerprocess.thread;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.huaouo.stormy.api.IBolt;
import com.huaouo.stormy.api.IOperator;
import com.huaouo.stormy.api.ISpout;
import com.huaouo.stormy.api.stream.DynamicSchema;
import com.huaouo.stormy.api.stream.OutputCollector;
import com.huaouo.stormy.api.stream.Tuple;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class ComputeThread implements Runnable {

    private IOperator operator;
    private DynamicSchema inboundSchema;
    private BlockingQueue<byte[]> inboundQueue;
    private OutputCollector outputCollector;

    public ComputeThread(Class<? extends IOperator> operatorClass,
                         DynamicSchema inboundSchema,
                         Map<String, DynamicSchema> outboundSchemaMap,
                         BlockingQueue<byte[]> inboundQueue,
                         Map<String, BlockingQueue<byte[]>> outboundQueueMap)
            throws IllegalAccessException, InstantiationException {
        this.operator = operatorClass.newInstance();
        this.inboundSchema = inboundSchema;
        this.inboundQueue = inboundQueue;

        this.outputCollector = new OutputCollector(outboundSchemaMap, outboundQueueMap);
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
            // TODO: add max tuple num constraint
            spout.nextTuple(outputCollector);
        }
    }

    private void boltLoop() {
        IBolt bolt = (IBolt) operator;
        while (true) {
            Tuple tuple;
            try {
                tuple = decodeInboundMessage();
            } catch (Throwable e) {
                continue;
            }
            bolt.compute(tuple, outputCollector);
        }
    }

    private Tuple decodeInboundMessage() throws InterruptedException, InvalidProtocolBufferException {
        byte[] messageBytes = inboundQueue.take();
        DynamicMessage.Builder parsedMessageBuilder = inboundSchema.newMessageBuilder("TupleData");
        Descriptors.Descriptor parsedMsgDesc = parsedMessageBuilder.getDescriptorForType();
        DynamicMessage parsedMessage = parsedMessageBuilder.mergeFrom(messageBytes).build();
        return new Tuple(parsedMessage, parsedMsgDesc);
    }
}
