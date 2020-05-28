package com.huaouo.stormy.workerprocess.acker;

import com.huaouo.stormy.workerprocess.thread.ComputedOutput;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class CachedComputedOutput {
    private int initTraceId;
    private String topologyName;
    private ComputedOutput computedOutput;
}
