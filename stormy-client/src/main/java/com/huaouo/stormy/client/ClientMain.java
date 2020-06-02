// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.client;

import com.huaouo.stormy.rpc.ManageTopologyRequestMetadata.RequestType;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

public class ClientMain {
    private static final int INVALID_COMMAND = 1;
    private static final int LACK_OF_ARGUMENTS = 2;

    public static void main(String[] args) throws Exception {
        if (args.length == 0 || !args[0].matches("start|list_running|stop")) {
            System.err.println("Usage:");
            System.err.println("  start <master_ip> <jar_file> <topology_name>");
            System.err.println("  list_running <master_ip>");
            System.err.println("  stop <master_ip> <topology_name>");
            System.exit(INVALID_COMMAND);
        }

        if (args.length < 2) {
            System.err.println("Lack of arguments");
            System.exit(LACK_OF_ARGUMENTS);
        }
        String target = args[1] + ":6000";
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
                jarFileStream = new FileInputStream(new File(args[2]));
                topologyName = args[3];
                break;
            case "list_running":
                requestType = RequestType.QUERY_RUNNING_TOPOLOGY;
                break;
            case "stop":
                if (args.length < 3) {
                    System.err.println("Lack of arguments");
                    System.exit(LACK_OF_ARGUMENTS);
                }
                requestType = RequestType.STOP_TOPOLOGY;
                topologyName = args[2];
                break;
        }

        ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext()
                .build();
        try {
            ManageTopologyClient client = new ManageTopologyClient(channel);
            System.out.println(client.manageTopology(requestType, topologyName, jarFileStream));
        } catch (IOException | InterruptedException e) {
            System.err.println("Failed while performing request:" + e.toString());
        } finally {
            if (jarFileStream != null) {
                jarFileStream.close();
            }
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
