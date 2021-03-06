// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.api.topology;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class EdgeDefinition {
    private String targetId;
    // This streamId is "taskName-realStreamId", different from the one returned
    // by OutputStreamDeclarer#getOutputStreamSchemas, which is "topologyName-taskName-realStreamId"
    private String streamId;
}
