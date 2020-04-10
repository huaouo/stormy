// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.api.topology;

import com.huaouo.stormy.api.IBolt;
import com.huaouo.stormy.api.ISpout;
import com.huaouo.stormy.api.util.SharedUtil;

import java.util.HashMap;
import java.util.Map;

public class TopologyDefinition {
    private String streamIdPrefix;
    private String spoutId;
    private ISpout spout;
    private Map<String, IBolt> bolts = new HashMap<>();

    public TopologyDefinition(String streamIdPrefix) {
        this.streamIdPrefix = streamIdPrefix;
    }

    public void setSpout(String id, ISpout spout) {
        SharedUtil.validateId(id);
        this.spoutId = spoutId;
        this.spout = spout;
    }

    public void addBolt(String id, IBolt bolt) {
        SharedUtil.validateId(id);
        bolts.put(id, bolt);
    }

    public void addStream(String sourceId, String targetId, String streamId) {
        SharedUtil.validateId(sourceId);
        SharedUtil.validateId(targetId);
        SharedUtil.validateId(streamId);

    }
}
