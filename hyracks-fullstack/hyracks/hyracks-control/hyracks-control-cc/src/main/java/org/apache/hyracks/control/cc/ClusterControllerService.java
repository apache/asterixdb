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
package org.apache.hyracks.control.cc;

import java.io.File;
import java.io.FileReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hyracks.api.application.ICCApplicationEntryPoint;
import org.apache.hyracks.api.client.ClusterControllerInfo;
import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.context.ICCContext;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.resource.DefaultJobCapacityController;
import org.apache.hyracks.api.job.resource.IJobCapacityController;
import org.apache.hyracks.api.service.IControllerService;
import org.apache.hyracks.api.topology.ClusterTopology;
import org.apache.hyracks.api.topology.TopologyDefinitionParser;
import org.apache.hyracks.control.cc.application.CCApplicationContext;
import org.apache.hyracks.control.cc.cluster.INodeManager;
import org.apache.hyracks.control.cc.cluster.NodeManager;
import org.apache.hyracks.control.cc.dataset.DatasetDirectoryService;
import org.apache.hyracks.control.cc.dataset.IDatasetDirectoryService;
import org.apache.hyracks.control.cc.job.IJobManager;
import org.apache.hyracks.control.cc.job.JobManager;
import org.apache.hyracks.control.cc.scheduler.IResourceManager;
import org.apache.hyracks.control.cc.scheduler.ResourceManager;
import org.apache.hyracks.control.cc.web.WebServer;
import org.apache.hyracks.control.cc.work.GatherStateDumpsWork.StateDumpRun;
import org.apache.hyracks.control.cc.work.GetIpAddressNodeNameMapWork;
import org.apache.hyracks.control.cc.work.GetThreadDumpWork.ThreadDumpRun;
import org.apache.hyracks.control.cc.work.RemoveDeadNodesWork;
import org.apache.hyracks.control.cc.work.ShutdownNCServiceWork;
import org.apache.hyracks.control.cc.work.TriggerNCWork;
import org.apache.hyracks.control.common.context.ServerContext;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.IniUtils;
import org.apache.hyracks.control.common.deployment.DeploymentRun;
import org.apache.hyracks.control.common.ipc.CCNCFunctions;
import org.apache.hyracks.control.common.logs.LogFile;
import org.apache.hyracks.control.common.shutdown.ShutdownRun;
import org.apache.hyracks.control.common.work.WorkQueue;
import org.apache.hyracks.ipc.api.IIPCI;
import org.apache.hyracks.ipc.impl.IPCSystem;
import org.apache.hyracks.ipc.impl.JavaSerializationBasedPayloadSerializerDeserializer;
import org.ini4j.Ini;
import org.xml.sax.InputSource;

public class ClusterControllerService implements IControllerService {
    private static final Logger LOGGER = Logger.getLogger(ClusterControllerService.class.getName());

    private final CCConfig ccConfig;

    private IPCSystem clusterIPC;

    private IPCSystem clientIPC;

    private final LogFile jobLog;

    private ServerContext serverCtx;

    private WebServer webServer;

    private ClusterControllerInfo info;

    private CCApplicationContext appCtx;

    private final PreDistributedJobStore preDistributedJobStore = new PreDistributedJobStore();

    private final WorkQueue workQueue;

    private ExecutorService executor;

    private final Timer timer;

    private final ICCContext ccContext;

    private final DeadNodeSweeper sweeper;

    private final IDatasetDirectoryService datasetDirectoryService;

    private final Map<DeploymentId, DeploymentRun> deploymentRunMap;

    private final Map<String, StateDumpRun> stateDumpRunMap;

    private final Map<String, ThreadDumpRun> threadDumpRunMap;

    private final INodeManager nodeManager;

    private final IResourceManager resourceManager = new ResourceManager();

    private IJobManager jobManager;

    private ShutdownRun shutdownCallback;

    private ICCApplicationEntryPoint aep;

    public ClusterControllerService(final CCConfig ccConfig) throws Exception {
        this.ccConfig = ccConfig;
        File jobLogFolder = new File(ccConfig.ccRoot, "logs/jobs");
        jobLog = new LogFile(jobLogFolder);

        // WorkQueue is in charge of heartbeat as well as other events.
        workQueue = new WorkQueue("ClusterController", Thread.MAX_PRIORITY);
        this.timer = new Timer(true);
        final ClusterTopology topology = computeClusterTopology(ccConfig);
        ccContext = new ClusterControllerContext(topology);
        sweeper = new DeadNodeSweeper();
        datasetDirectoryService = new DatasetDirectoryService(ccConfig.resultTTL, ccConfig.resultSweepThreshold);

        deploymentRunMap = new HashMap<>();
        stateDumpRunMap = new HashMap<>();
        threadDumpRunMap = Collections.synchronizedMap(new HashMap<>());

        // Node manager is in charge of cluster membership management.
        nodeManager = new NodeManager(ccConfig, resourceManager);
    }

    private static ClusterTopology computeClusterTopology(CCConfig ccConfig) throws Exception {
        if (ccConfig.clusterTopologyDefinition == null) {
            return null;
        }
        FileReader fr = new FileReader(ccConfig.clusterTopologyDefinition);
        InputSource in = new InputSource(fr);
        try {
            return TopologyDefinitionParser.parse(in);
        } finally {
            fr.close();
        }
    }

    @Override
    public void start() throws Exception {
        LOGGER.log(Level.INFO, "Starting ClusterControllerService: " + this);
        serverCtx = new ServerContext(ServerContext.ServerType.CLUSTER_CONTROLLER, new File(ccConfig.ccRoot));
        IIPCI ccIPCI = new ClusterControllerIPCI(this);
        clusterIPC = new IPCSystem(new InetSocketAddress(ccConfig.clusterNetPort), ccIPCI,
                new CCNCFunctions.SerializerDeserializer());
        IIPCI ciIPCI = new ClientInterfaceIPCI(this);
        clientIPC = new IPCSystem(new InetSocketAddress(ccConfig.clientNetIpAddress, ccConfig.clientNetPort), ciIPCI,
                new JavaSerializationBasedPayloadSerializerDeserializer());
        webServer = new WebServer(this, ccConfig.httpPort);
        clusterIPC.start();
        clientIPC.start();
        webServer.start();
        info = new ClusterControllerInfo(ccConfig.clientNetIpAddress, ccConfig.clientNetPort,
                webServer.getListeningPort());
        timer.schedule(sweeper, 0, ccConfig.heartbeatPeriod);
        jobLog.open();
        startApplication();

        datasetDirectoryService.init(executor);
        workQueue.start();
        connectNCs();
        LOGGER.log(Level.INFO, "Started ClusterControllerService");
        notifyApplication();
    }

    private void startApplication() throws Exception {
        appCtx = new CCApplicationContext(this, serverCtx, ccContext, ccConfig.getAppConfig());
        appCtx.addJobLifecycleListener(datasetDirectoryService);
        executor = Executors.newCachedThreadPool(appCtx.getThreadFactory());
        String className = ccConfig.appCCMainClass;

        IJobCapacityController jobCapacityController = DefaultJobCapacityController.INSTANCE;
        if (className != null) {
            Class<?> c = Class.forName(className);
            aep = (ICCApplicationEntryPoint) c.newInstance();
            String[] args = ccConfig.appArgs == null ? null
                    : ccConfig.appArgs.toArray(new String[ccConfig.appArgs.size()]);
            aep.start(appCtx, args);
            jobCapacityController = aep.getJobCapacityController();
        }

        // Job manager is in charge of job lifecycle management.
        try {
            Constructor<?> jobManagerConstructor = this.getClass().getClassLoader()
                    .loadClass(ccConfig.jobManagerClassName)
                    .getConstructor(CCConfig.class, ClusterControllerService.class, IJobCapacityController.class);
            jobManager = (IJobManager) jobManagerConstructor.newInstance(ccConfig, this, jobCapacityController);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException
                | InvocationTargetException e) {
            if (LOGGER.isLoggable(Level.WARNING)) {
                LOGGER.log(Level.WARNING, "class " + ccConfig.jobManagerClassName + " could not be used: ", e);
            }
            // Falls back to the default implementation if the user-provided class name is not valid.
            jobManager = new JobManager(ccConfig, this, jobCapacityController);
        }
    }

    private void connectNCs() throws Exception {
        Ini ini = ccConfig.getIni();
        if (ini == null || Boolean.parseBoolean(ini.get("cc", "virtual.cluster"))) {
            return;
        }
        for (String section : ini.keySet()) {
            if (!section.startsWith("nc/")) {
                continue;
            }
            String ncid = section.substring(3);
            String address = IniUtils.getString(ini, section, "address", null);
            int port = IniUtils.getInt(ini, section, "port", 9090);
            if (address == null) {
                address = InetAddress.getLoopbackAddress().getHostAddress();
            }
            workQueue.schedule(new TriggerNCWork(this, address, port, ncid));
        }
    }

    private void terminateNCServices() throws Exception {
        Ini ini = ccConfig.getIni();
        if (ini == null || Boolean.parseBoolean(ini.get("cc", "virtual.cluster"))) {
            return;
        }
        List<ShutdownNCServiceWork> shutdownNCServiceWorks = new ArrayList<>();
        for (String section : ini.keySet()) {
            if (!section.startsWith("nc/")) {
                continue;
            }
            String ncid = section.substring(3);
            String address = IniUtils.getString(ini, section, "address", null);
            int port = IniUtils.getInt(ini, section, "port", 9090);
            if (address == null) {
                address = InetAddress.getLoopbackAddress().getHostAddress();
            }
            ShutdownNCServiceWork shutdownWork = new ShutdownNCServiceWork(address, port, ncid);
            workQueue.schedule(shutdownWork);
            shutdownNCServiceWorks.add(shutdownWork);
        }
        for (ShutdownNCServiceWork shutdownWork : shutdownNCServiceWorks) {
            shutdownWork.sync();
        }
    }

    private void notifyApplication() throws Exception {
        if (aep != null) {
            // Sometimes, there is no application entry point. Check hyracks-client project
            aep.startupCompleted();
        }
    }
    public void stop(boolean terminateNCService) throws Exception {
        if (terminateNCService) {
            terminateNCServices();
        }
        stop();
    }

    @Override
    public void stop() throws Exception {
        LOGGER.log(Level.INFO, "Stopping ClusterControllerService");
        stopApplication();
        webServer.stop();
        sweeper.cancel();
        workQueue.stop();
        executor.shutdownNow();
        clusterIPC.stop();
        jobLog.close();
        clientIPC.stop();
        LOGGER.log(Level.INFO, "Stopped ClusterControllerService");
    }

    private void stopApplication() throws Exception {
        if (aep != null) {
            aep.stop();
        }
    }

    public ServerContext getServerContext() {
        return serverCtx;
    }

    public ICCContext getCCContext() {
        return ccContext;
    }

    public IJobManager getJobManager() {
        return jobManager;
    }

    public INodeManager getNodeManager() {
        return nodeManager;
    }

    public PreDistributedJobStore getPreDistributedJobStore() throws HyracksException {
        return preDistributedJobStore;
    }

    public IResourceManager getResourceManager() {
        return resourceManager;
    }

    public LogFile getJobLogFile() {
        return jobLog;
    }

    public WorkQueue getWorkQueue() {
        return workQueue;
    }

    public ExecutorService getExecutorService() {
        return executor;
    }

    public Executor getExecutor() {
        return getExecutorService();
    }

    public CCConfig getConfig() {
        return ccConfig;
    }

    public CCApplicationContext getApplicationContext() {
        return appCtx;
    }

    public ClusterControllerInfo getClusterControllerInfo() {
        return info;
    }

    public CCConfig getCCConfig() {
        return ccConfig;
    }

    public IPCSystem getClusterIPC() {
        return clusterIPC;
    }

    public NetworkAddress getDatasetDirectoryServiceInfo() {
        return new NetworkAddress(ccConfig.clientNetIpAddress, ccConfig.clientNetPort);
    }

    private final class ClusterControllerContext implements ICCContext {
        private final ClusterTopology topology;

        private ClusterControllerContext(ClusterTopology topology) {
            this.topology = topology;
        }

        @Override
        public void getIPAddressNodeMap(Map<InetAddress, Set<String>> map) throws HyracksDataException {
            GetIpAddressNodeNameMapWork ginmw = new GetIpAddressNodeNameMapWork(
                    ClusterControllerService.this.getNodeManager(), map);
            try {
                workQueue.scheduleAndSync(ginmw);
            } catch (Exception e) {
                throw new HyracksDataException(e);
            }
        }

        @Override
        public ClusterControllerInfo getClusterControllerInfo() {
            return info;
        }

        @Override
        public ClusterTopology getClusterTopology() {
            return topology;
        }
    }

    private class DeadNodeSweeper extends TimerTask {
        @Override
        public void run() {
            workQueue.schedule(new RemoveDeadNodesWork(ClusterControllerService.this));
        }
    }

    public IDatasetDirectoryService getDatasetDirectoryService() {
        return datasetDirectoryService;
    }

    public synchronized void addStateDumpRun(String id, StateDumpRun sdr) {
        stateDumpRunMap.put(id, sdr);
    }

    public synchronized StateDumpRun getStateDumpRun(String id) {
        return stateDumpRunMap.get(id);
    }

    public synchronized void removeStateDumpRun(String id) {
        stateDumpRunMap.remove(id);
    }

    /**
     * Add a deployment run
     *
     * @param deploymentKey
     * @param dRun
     */
    public synchronized void addDeploymentRun(DeploymentId deploymentKey, DeploymentRun dRun) {
        deploymentRunMap.put(deploymentKey, dRun);
    }

    /**
     * Get a deployment run
     *
     * @param deploymentKey
     */
    public synchronized DeploymentRun getDeploymentRun(DeploymentId deploymentKey) {
        return deploymentRunMap.get(deploymentKey);
    }

    /**
     * Remove a deployment run
     *
     * @param deploymentKey
     */
    public synchronized void removeDeploymentRun(DeploymentId deploymentKey) {
        deploymentRunMap.remove(deploymentKey);
    }

    public synchronized void setShutdownRun(ShutdownRun sRun) {
        shutdownCallback = sRun;
    }

    public synchronized ShutdownRun getShutdownRun() {
        return shutdownCallback;
    }

    public void addThreadDumpRun(String requestKey, ThreadDumpRun tdr) {
        threadDumpRunMap.put(requestKey, tdr);
    }

    public ThreadDumpRun removeThreadDumpRun(String requestKey) {
        return threadDumpRunMap.remove(requestKey);
    }
}
