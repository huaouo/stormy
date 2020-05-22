// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.workerprocess.topology;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.huaouo.stormy.workerprocess.thread.ComputedOutput;

@FunctionalInterface
public interface BeforeEmitCallback {

    ComputedOutput accept(DynamicMessage.Builder msgBuilder, Descriptors.Descriptor msgDesc, String targetStreamId);
}
