// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.master.util;

import java.util.Map;

public class MasterUtil {

    private static final String START_INDENT = "  ";
    private static final String STOP_INDENT = START_INDENT + " ";

    public static String formatRunningTopologies(Map<String, String> runningTopologies) {
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, String> e : runningTopologies.entrySet()) {
            builder.append(e.getValue());
            if ("START".equals(e.getValue())) {
                builder.append(START_INDENT);
            } else { // "STOP".equals(e.getValue())
                builder.append(STOP_INDENT);
            }
            builder.append(e.getKey());
            builder.append('\n');
        }
        return builder.toString();
    }
}
