// Copyright 2020 Zhenhua Yang
// Licensed under the MIT License.

package com.huaouo.stormy.worker;

import com.huaouo.stormy.rpc.ProvideJarGrpc;
import com.huaouo.stormy.rpc.ProvideJarGrpc.ProvideJarStub;
import com.huaouo.stormy.rpc.ProvideJarRequest;
import com.huaouo.stormy.rpc.ProvideJarResponse;
import com.huaouo.stormy.shared.util.SharedUtil;
import com.huaouo.stormy.shared.wrapper.ZooKeeperConnection;
import com.huaouo.stormy.worker.service.JarFileService;
import com.huaouo.stormy.worker.util.WorkerUtil;
import com.huaouo.stormy.workerprocess.WorkerProcessMain;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Singleton
public class WorkerServer {

    @Inject
    private ZooKeeperConnection zkConn;

    @Inject
    private JarFileService jarService;

    private ProvideJarStub grpcStub;

    private String registeredPath;
    private String nodeDataPath;
    private String acceptedTasksPath;

    public void startAndBlock() {
        String ip = null;
        try {
            ip = SharedUtil.getIp();
        } catch (IOException e) {
            log.error("Cannot get host IP: " + e.toString());
            System.exit(-1);
        }

        registeredPath = "/worker/registered/" + ip;
        nodeDataPath = "/worker/nodeData/" + ip;
        acceptedTasksPath = nodeDataPath + "/accepted";
        zkConn.create(registeredPath, "0");
        zkConn.create(nodeDataPath, null);
        if (!zkConn.create("/worker/available/" + ip, null, CreateMode.EPHEMERAL)) {
            log.error("A worker is already running on this node");
            System.exit(-1);
        }

        initGrpcClient(zkConn.get("/master"));

        handleAssignmentChange();
        zkConn.addWatch(registeredPath, e -> {
            if (e.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                handleAssignmentChange();
            }
        });

        // TODO: add restart logic for worker process

        // Block
        synchronized (this) {
            try {
                this.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void initGrpcClient(String target) {
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext()
                .build();
        grpcStub = ProvideJarGrpc.newStub(channel);
    }

    private void handleAssignmentChange() {
        List<String> assignedTasks = zkConn.getChildren(registeredPath);
        List<String> acceptedTasks = zkConn.getChildren(acceptedTasksPath);
        assignedTasks.forEach(t -> {
            if (!acceptedTasks.contains(t)) {
                acceptTask(t);
            }
        });
        acceptedTasks.forEach(t -> {
            if (!assignedTasks.contains(t)) {
                removeTask(t);
            }
        });
    }

    private void acceptTask(String taskFullName) {
        // [0] => topologyName
        // [1] => taskName
        // [2] => processIndex
        // [3] => inboundStr
        // [4] => outboundStr
        String[] taskInfo = taskFullName.split("#");
        String topologyName = taskInfo[0];

        String jarPath = null;
        try {
            jarPath = getJarPath(topologyName);
        } catch (Throwable t) {
            log.error("Failed to get jar: " + t.toString());
            System.exit(-1);
        }

        // .substring(6) => remove "file:/" prefix
        String currentJarPath = getClass().getProtectionDomain().getCodeSource()
                .getLocation().toString().substring(6);
        String command = WorkerUtil.getJvmPath() + "-cp " + currentJarPath + " " +
                WorkerProcessMain.class.getCanonicalName() + " " + jarPath + " " + taskFullName;
        log.info("Execute \"" + command + "\"");
        long pid = -1;
        try {
            Process p = Runtime.getRuntime().exec(command);
            pid = WorkerUtil.getPid(p);
        } catch (Throwable t) {
            log.error("Failed to launch worker process: " + t.toString());
            System.exit(-1);
        }
        zkConn.create(acceptedTasksPath + "/" + taskFullName, Long.toString(pid));
    }

    private void removeTask(String taskFullName) {
        String removeTaskPath = acceptedTasksPath + "/" + taskFullName;
        long pid = Long.parseLong(zkConn.get(removeTaskPath));
        WorkerUtil.killByPid(pid);
        zkConn.delete(removeTaskPath);
    }

    private String getJarPath(String topologyName) throws InterruptedException {
        while (!jarService.jarFileExists(topologyName)) {
            OutputStream out;
            try {
                out = jarService.getOutputStream(topologyName);
            } catch (IOException e) {
                log.error("Fail to write jar file: " + e.toString());
                continue;
            }
            Runnable closeOutputStream = () -> {
                try {
                    out.close();
                } catch (IOException e) {
                    log.error("Fail to write jar file: " + e.toString());
                }
            };

            ProvideJarRequest request = ProvideJarRequest.newBuilder()
                    .setTopologyName(topologyName)
                    .build();

            CountDownLatch receiveCompleted = new CountDownLatch(1);
            AtomicBoolean success = new AtomicBoolean();
            grpcStub.withCompression("gzip").provideJar(request, new StreamObserver<ProvideJarResponse>() {
                @Override
                public void onNext(ProvideJarResponse value) {
                    try {
                        out.write(value.getJarBytes().toByteArray());
                    } catch (IOException e) {
                        onError(e);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    log.error("Fail to write jar file: " + t.toString());
                    closeOutputStream.run();
                    receiveCompleted.countDown();
                }

                @Override
                public void onCompleted() {
                    success.set(true);
                    closeOutputStream.run();
                    receiveCompleted.countDown();
                }
            });

            receiveCompleted.await();
        }
        return jarService.getJarFilePath(topologyName).toString();
    }
}
