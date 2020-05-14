// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.workerprocess.thread;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ComputedOutput {

    private String streamId;
    private byte[] bytes;
}
