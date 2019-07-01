/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hyracks.ipc.impl;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.hyracks.api.client.ClusterControllerInfo;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.client.IHyracksClientInterface;
import org.apache.hyracks.api.client.NodeControllerInfo;
import org.apache.hyracks.api.client.impl.JobSpecificationActivityClusterGraphGeneratorFactory;
import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.DeployedJobSpecId;
import org.apache.hyracks.api.job.IActivityClusterGraphGeneratorFactory;
import org.apache.hyracks.api.job.JobFlag;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobInfo;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.network.ISocketChannelFactory;
import org.apache.hyracks.api.topology.ClusterTopology;
import org.apache.hyracks.api.util.InvokeUtil;
import org.apache.hyracks.api.util.JavaSerializationUtils;
import org.apache.hyracks.ipc.api.RPCInterface;
import org.apache.hyracks.ipc.sockets.PlainSocketChannelFactory;
import org.apache.hyracks.util.ExitUtil;
import org.apache.hyracks.util.InterruptibleAction;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Connection Class used by a Hyracks Client to interact with a Hyracks Cluster
 * Controller.
 *
 * @author vinayakb
 */
public final class HyracksConnection implements IHyracksClientConnection {

    private static final Logger LOGGER = LogManager.getLogger();

    private final String ccHost;

    private final int ccPort;

    private final IPCSystem ipc;

    private final IHyracksClientInterface hci;

    private final ClusterControllerInfo ccInfo;

    private volatile boolean running = false;

    private volatile long reqId = 0L;

    private final ExecutorService uninterruptibleExecutor =
            Executors.newFixedThreadPool(2, r -> new Thread(r, "HyracksConnection Uninterrubtible thread: "));

    private final BlockingQueue<UnInterruptibleRequest<?>> uninterruptibles = new ArrayBlockingQueue<>(1);

    /**
     * Constructor to create a connection to the Hyracks Cluster Controller.
     *
     * @param ccHost
     *            Host name (or IP Address) where the Cluster Controller can be
     *            reached.
     * @param ccPort
     *            Port to reach the Hyracks Cluster Controller at the specified
     *            host name.
     * @throws Exception
     */
    public HyracksConnection(String ccHost, int ccPort, ISocketChannelFactory socketChannelFactory) throws Exception {
        this.ccHost = ccHost;
        this.ccPort = ccPort;
        RPCInterface rpci = new RPCInterface();
        ipc = new IPCSystem(new InetSocketAddress(0), socketChannelFactory, rpci,
                new JavaSerializationBasedPayloadSerializerDeserializer());
        ipc.start();
        hci = new HyracksClientInterfaceRemoteProxy(ipc.getReconnectingHandle(new InetSocketAddress(ccHost, ccPort)),
                rpci);
        ccInfo = hci.getClusterControllerInfo();
        uninterruptibleExecutor.execute(new UninterrubtileRequestHandler());
        uninterruptibleExecutor.execute(new UninterrubtileHandlerWatcher());
    }

    public HyracksConnection(String ccHost, int ccPort) throws Exception {
        this(ccHost, ccPort, PlainSocketChannelFactory.INSTANCE);
    }

    @Override
    public JobStatus getJobStatus(JobId jobId) throws Exception {
        return hci.getJobStatus(jobId);
    }

    @Override
    public void cancelJob(JobId jobId) throws Exception {
        CancelJobRequest request = new CancelJobRequest(jobId);
        uninterruptiblySubmitAndExecute(request);
    }

    @Override
    public JobId startJob(JobSpecification jobSpec) throws Exception {
        return startJob(jobSpec, EnumSet.noneOf(JobFlag.class));
    }

    @Override
    public JobId startJob(JobSpecification jobSpec, EnumSet<JobFlag> jobFlags) throws Exception {
        IActivityClusterGraphGeneratorFactory jsacggf =
                new JobSpecificationActivityClusterGraphGeneratorFactory(jobSpec);
        return startJob(jsacggf, jobFlags);
    }

    @Override
    public void redeployJobSpec(DeployedJobSpecId deployedJobSpecId, JobSpecification jobSpec) throws Exception {
        JobSpecificationActivityClusterGraphGeneratorFactory jsacggf =
                new JobSpecificationActivityClusterGraphGeneratorFactory(jobSpec);
        hci.redeployJobSpec(deployedJobSpecId, JavaSerializationUtils.serialize(jsacggf));
    }

    @Override
    public DeployedJobSpecId deployJobSpec(JobSpecification jobSpec) throws Exception {
        JobSpecificationActivityClusterGraphGeneratorFactory jsacggf =
                new JobSpecificationActivityClusterGraphGeneratorFactory(jobSpec);
        return deployJobSpec(jsacggf);
    }

    @Override
    public void undeployJobSpec(DeployedJobSpecId deployedJobSpecId) throws Exception {
        hci.undeployJobSpec(deployedJobSpecId);
    }

    @Override
    public JobId startJob(DeployedJobSpecId deployedJobSpecId, Map<byte[], byte[]> jobParameters) throws Exception {
        StartDeployedJobRequest request = new StartDeployedJobRequest(deployedJobSpecId, jobParameters);
        return interruptiblySubmitAndExecute(request);
    }

    @Override
    public JobId startJob(IActivityClusterGraphGeneratorFactory acggf, EnumSet<JobFlag> jobFlags) throws Exception {
        return startJob(null, acggf, jobFlags);
    }

    public DeployedJobSpecId deployJobSpec(IActivityClusterGraphGeneratorFactory acggf) throws Exception {
        return hci.deployJobSpec(JavaSerializationUtils.serialize(acggf));
    }

    @Override
    public NetworkAddress getResultDirectoryAddress() throws Exception {
        return hci.getResultDirectoryAddress();
    }

    @Override
    public void waitForCompletion(JobId jobId) throws Exception {
        try {
            hci.waitForCompletion(jobId);
        } catch (InterruptedException e) {
            // Cancels an on-going job if the current thread gets interrupted.
            cancelJob(jobId);
            throw e;
        }
    }

    @Override
    public Map<String, NodeControllerInfo> getNodeControllerInfos() throws HyracksException {
        try {
            return hci.getNodeControllersInfo();
        } catch (Exception e) {
            throw HyracksException.create(e);
        }
    }

    @Override
    public ClusterTopology getClusterTopology() throws HyracksException {
        try {
            return hci.getClusterTopology();
        } catch (Exception e) {
            throw HyracksException.create(e);
        }
    }

    @Override
    public DeploymentId deployBinary(List<String> files) throws Exception {
        /** generate a deployment id */
        DeploymentId deploymentId = new DeploymentId(UUID.randomUUID().toString());
        deployBinary(deploymentId, files, false);
        return deploymentId;
    }

    @Override
    public void deployBinary(DeploymentId deploymentId, List<String> files, boolean extractFromArchive)
            throws Exception {
        List<URL> binaryURLs = new ArrayList<>();
        if (files != null && !files.isEmpty()) {
            CloseableHttpClient hc = new DefaultHttpClient();
            try {
                /** upload files through a http client one-by-one to the CC server */
                for (String file : files) {
                    int slashIndex = file.lastIndexOf('/');
                    String fileName = file.substring(slashIndex + 1);
                    String url = "http://" + ccHost + ":" + ccInfo.getWebPort() + "/applications/"
                            + deploymentId.toString() + "&" + fileName;
                    HttpPut put = new HttpPut(url);
                    put.setEntity(new FileEntity(new File(file), "application/octet-stream"));
                    HttpResponse response = hc.execute(put);
                    response.getEntity().consumeContent();
                    if (response.getStatusLine().getStatusCode() != 200) {
                        hci.unDeployBinary(deploymentId);
                        throw new HyracksException(response.getStatusLine().toString());
                    }
                    /** add the uploaded URL address into the URLs of jars to be deployed at NCs */
                    binaryURLs.add(new URL(url));
                }
            } finally {
                hc.close();
            }
        }
        /** deploy the URLs to the CC and NCs */
        hci.deployBinary(binaryURLs, deploymentId, extractFromArchive);
    }

    @Override
    public void unDeployBinary(DeploymentId deploymentId) throws Exception {
        hci.unDeployBinary(deploymentId);
    }

    @Override
    public JobId startJob(DeploymentId deploymentId, JobSpecification jobSpec) throws Exception {
        return startJob(deploymentId, jobSpec, EnumSet.noneOf(JobFlag.class));
    }

    @Override
    public JobId startJob(DeploymentId deploymentId, JobSpecification jobSpec, EnumSet<JobFlag> jobFlags)
            throws Exception {
        IActivityClusterGraphGeneratorFactory jsacggf =
                new JobSpecificationActivityClusterGraphGeneratorFactory(jobSpec);
        return startJob(deploymentId, jsacggf, jobFlags);
    }

    @Override
    public JobId startJob(DeploymentId deploymentId, IActivityClusterGraphGeneratorFactory acggf,
            EnumSet<JobFlag> jobFlags) throws Exception {
        StartJobRequest request = new StartJobRequest(deploymentId, acggf, jobFlags);
        return interruptiblySubmitAndExecute(request);
    }

    @Override
    public JobInfo getJobInfo(JobId jobId) throws Exception {
        return hci.getJobInfo(jobId);
    }

    @Override
    public void stopCluster(boolean terminateNCService) throws Exception {
        hci.stopCluster(terminateNCService);
    }

    @Override
    public String getNodeDetailsJSON(String nodeId, boolean includeStats, boolean includeConfig) throws Exception {
        return hci.getNodeDetailsJSON(nodeId, includeStats, includeConfig);
    }

    @Override
    public String getThreadDump(String node) throws Exception {
        return hci.getThreadDump(node);
    }

    @Override
    public String getHost() {
        return ccHost;
    }

    @Override
    public int getPort() {
        return ccPort;
    }

    @Override
    public boolean isConnected() {
        return hci.isConnected();
    }

    private <T> T uninterruptiblySubmitAndExecute(UnInterruptibleRequest<T> request) throws Exception {
        InvokeUtil.doUninterruptibly(() -> uninterruptibles.put(request));
        return uninterruptiblyExecute(request);
    }

    private <T> T uninterruptiblyExecute(UnInterruptibleRequest<T> request) throws Exception {
        InvokeUtil.doUninterruptibly(request);
        return request.result();
    }

    private <T> T interruptiblySubmitAndExecute(UnInterruptibleRequest<T> request) throws Exception {
        uninterruptibles.put(request);
        return uninterruptiblyExecute(request);
    }

    private abstract class UnInterruptibleRequest<T> implements InterruptibleAction {
        boolean completed = false;
        boolean failed = false;
        Throwable failure = null;
        T response = null;

        @SuppressWarnings("squid:S1181")
        private final void handle() {
            try {
                response = doHandle();
            } catch (Throwable th) {
                failed = true;
                failure = th;
            } finally {
                synchronized (this) {
                    completed = true;
                    notifyAll();
                }
            }
        }

        protected abstract T doHandle() throws Exception;

        @Override
        public final synchronized void run() throws InterruptedException {
            while (!completed) {
                wait();
            }
        }

        public T result() throws Exception {
            if (failed) {
                if (failure instanceof Error) {
                    throw (Error) failure;
                }
                throw (Exception) failure;
            }
            return response;
        }
    }

    private class CancelJobRequest extends UnInterruptibleRequest<Void> {
        final JobId jobId;

        public CancelJobRequest(JobId jobId) {
            this.jobId = jobId;
        }

        @Override
        protected Void doHandle() throws Exception {
            hci.cancelJob(jobId);
            return null;
        }

        @Override
        public String toString() {
            return "CancelJobRequest: " + jobId.toString();
        }

    }

    private class StartDeployedJobRequest extends UnInterruptibleRequest<JobId> {

        private final DeployedJobSpecId deployedJobSpecId;
        private final Map<byte[], byte[]> jobParameters;

        public StartDeployedJobRequest(DeployedJobSpecId deployedJobSpecId, Map<byte[], byte[]> jobParameters) {
            this.deployedJobSpecId = deployedJobSpecId;
            this.jobParameters = jobParameters;
        }

        @Override
        protected JobId doHandle() throws Exception {
            return hci.startJob(deployedJobSpecId, jobParameters);
        }

    }

    private class StartJobRequest extends UnInterruptibleRequest<JobId> {
        private final DeploymentId deploymentId;
        private final IActivityClusterGraphGeneratorFactory acggf;
        private final EnumSet<JobFlag> jobFlags;

        public StartJobRequest(DeploymentId deploymentId, IActivityClusterGraphGeneratorFactory acggf,
                EnumSet<JobFlag> jobFlags) {
            this.deploymentId = deploymentId;
            this.acggf = acggf;
            this.jobFlags = jobFlags;
        }

        @Override
        protected JobId doHandle() throws Exception {
            if (deploymentId == null) {
                return hci.startJob(JavaSerializationUtils.serialize(acggf), jobFlags);
            } else {
                return hci.startJob(deploymentId, JavaSerializationUtils.serialize(acggf), jobFlags);
            }
        }

        @Override
        public String toString() {
            return "StartJobRequest";
        }

    }

    private class UninterrubtileRequestHandler implements Runnable {
        @SuppressWarnings({ "squid:S2189", "squid:S2142" })
        @Override
        public void run() {
            String nameBefore = Thread.currentThread().getName();
            Thread.currentThread().setName(nameBefore + getClass().getSimpleName());
            try {
                while (true) {
                    try {
                        UnInterruptibleRequest<?> current = uninterruptibles.take();
                        reqId++;
                        running = true;
                        current.handle();
                    } catch (InterruptedException e) {
                        LOGGER.log(Level.WARN, "Ignoring interrupt. This thread should never be interrupted.");
                        continue;
                    } finally {
                        running = false;
                    }
                }
            } finally {
                Thread.currentThread().setName(nameBefore);
            }
        }
    }

    public class UninterrubtileHandlerWatcher implements Runnable {
        @Override
        @SuppressWarnings({ "squid:S2189", "squid:S2142" })
        public void run() {
            String nameBefore = Thread.currentThread().getName();
            Thread.currentThread().setName(nameBefore + getClass().getSimpleName());
            try {
                long currentReqId = 0L;
                long currentTime = System.nanoTime();
                while (true) {
                    try {
                        TimeUnit.MINUTES.sleep(1);
                    } catch (InterruptedException e) {
                        LOGGER.log(Level.WARN, "Ignoring interrupt. This thread should never be interrupted.");
                        continue;
                    }
                    if (running) {
                        if (reqId == currentReqId) {
                            if (TimeUnit.NANOSECONDS.toMinutes(System.nanoTime() - currentTime) > 0) {
                                ExitUtil.halt(ExitUtil.EC_FAILED_TO_PROCESS_UN_INTERRUPTIBLE_REQUEST);
                            }
                        } else {
                            currentReqId = reqId;
                            currentTime = System.nanoTime();
                        }
                    }
                }
            } finally {
                Thread.currentThread().setName(nameBefore);
            }
        }
    }
}
