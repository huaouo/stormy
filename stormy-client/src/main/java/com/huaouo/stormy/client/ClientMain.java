// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.client;

import com.huaouo.stormy.rpc.ManageTopologyRequest.RequestType;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

public class ClientMain {
    private static final int INVALID_COMMAND = 1;
    private static final int LACK_OF_ARGUMENTS = 2;

    public static void main(String[] args) throws Exception {
        if (args.length == 0 || !args[0].matches("start|status|stop")) {
            System.err.println("Usage:");
            System.err.println("start <nimbus_ip> <jarFile> <topologyName>");
            System.err.println("status <nimbus_ip>");
            System.err.println("stop <nimbus_ip> <topologyName>");
            System.exit(INVALID_COMMAND);
        }

        String target = ":5000";
        RequestType requestType = RequestType.UNRECOGNIZED;
        InputStream jarFileStream = null;
        String topologyName = null;

        switch (args[0]) {
            case "start":
                if (args.length < 4) {
                    System.err.println("Lack of arguments");
                    System.exit(LACK_OF_ARGUMENTS);
                }
                requestType = RequestType.START_TOPOLOGY;
                target = args[1] + target;
                jarFileStream = new FileInputStream(new File(args[2]));
                topologyName = args[3];
                break;
            case "status":
                if (args.length < 2) {
                    System.err.println("Lack of arguments");
                    System.exit(LACK_OF_ARGUMENTS);
                }
                requestType = RequestType.QUERY_TOPOLOGY_STATUS;
                target = args[1] + target;
                break;
            case "stop":
                if (args.length < 3) {
                    System.err.println("Lack of arguments");
                    System.exit(LACK_OF_ARGUMENTS);
                }
                requestType = RequestType.STOP_TOPOLOGY;
                target = args[1] + target;
                topologyName = args[2];
                break;
        }

        ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext(true)
                .build();
        try {
            ManageTopologyClient client = new ManageTopologyClient(channel);
            System.out.println(client.manageTopology(requestType, topologyName, jarFileStream));
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
