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
import com.huaouo.stormy.workerprocess.topology.OutputCollectorImpl;
import com.huaouo.stormy.api.stream.Tuple;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

@Slf4j
public class ComputeThread implements Runnable {

    private IOperator operator;
    private DynamicSchema inboundSchema;
    private BlockingQueue<byte[]> inboundQueue;
    private OutputCollectorImpl outputCollector;

    public ComputeThread(Class<? extends IOperator> operatorClass,
                         DynamicSchema inboundSchema,
                         Map<String, DynamicSchema> outboundSchemaMap,
                         BlockingQueue<byte[]> inboundQueue,
                         BlockingQueue<ComputedOutput> outboundQueue)
            throws IllegalAccessException, InstantiationException {
        this.operator = operatorClass.newInstance();
        this.inboundSchema = inboundSchema;
        this.inboundQueue = inboundQueue;

        this.outputCollector = new OutputCollectorImpl(outboundSchemaMap, outboundQueue);
    }

    // TODO: check if process will exit if this thread
    //       throws an exception
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
            try {
                // TODO: add max tuple num constraint
                spout.nextTuple(outputCollector);
            } catch (Throwable t) {
                log.error(t.toString());
            }
        }
    }

    private void boltLoop() {
        IBolt bolt = (IBolt) operator;
        while (true) {
            Tuple tuple;
            try {
                tuple = decodeInboundMessage();
                bolt.compute(tuple, outputCollector);
            } catch (Throwable t) {
                log.error(t.toString());
            }
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
