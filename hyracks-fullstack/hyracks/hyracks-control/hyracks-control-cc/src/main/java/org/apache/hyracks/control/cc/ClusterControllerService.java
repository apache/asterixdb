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
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;

import org.apache.hyracks.api.application.ICCApplication;
import org.apache.hyracks.api.client.ClusterControllerInfo;
import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.config.IApplicationConfig;
import org.apache.hyracks.api.context.ICCContext;
import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.DeployedJobSpecIdFactory;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobIdFactory;
import org.apache.hyracks.api.job.JobParameterByteStore;
import org.apache.hyracks.api.job.resource.IJobCapacityController;
import org.apache.hyracks.api.network.INetworkSecurityConfig;
import org.apache.hyracks.api.network.INetworkSecurityManager;
import org.apache.hyracks.api.service.IControllerService;
import org.apache.hyracks.api.topology.ClusterTopology;
import org.apache.hyracks.api.topology.TopologyDefinitionParser;
import org.apache.hyracks.control.cc.application.CCServiceContext;
import org.apache.hyracks.control.cc.cluster.INodeManager;
import org.apache.hyracks.control.cc.cluster.NodeManager;
import org.apache.hyracks.control.cc.job.IJobManager;
import org.apache.hyracks.control.cc.job.JobManager;
import org.apache.hyracks.control.cc.result.IResultDirectoryService;
import org.apache.hyracks.control.cc.result.ResultDirectoryService;
import org.apache.hyracks.control.cc.scheduler.IResourceManager;
import org.apache.hyracks.control.cc.scheduler.ResourceManager;
import org.apache.hyracks.control.cc.web.WebServer;
import org.apache.hyracks.control.cc.work.GatherStateDumpsWork.StateDumpRun;
import org.apache.hyracks.control.cc.work.GetIpAddressNodeNameMapWork;
import org.apache.hyracks.control.cc.work.GetThreadDumpWork.ThreadDumpRun;
import org.apache.hyracks.control.cc.work.RemoveDeadNodesWork;
import org.apache.hyracks.control.cc.work.ShutdownNCServiceWork;
import org.apache.hyracks.control.cc.work.TriggerNCWork;
import org.apache.hyracks.control.common.config.ConfigManager;
import org.apache.hyracks.control.common.context.ServerContext;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.common.deployment.DeploymentRun;
import org.apache.hyracks.control.common.ipc.CCNCFunctions;
import org.apache.hyracks.control.common.logs.LogFile;
import org.apache.hyracks.control.common.shutdown.ShutdownRun;
import org.apache.hyracks.control.common.work.WorkQueue;
import org.apache.hyracks.ipc.api.IIPCI;
import org.apache.hyracks.ipc.impl.IPCSystem;
import org.apache.hyracks.ipc.impl.JavaSerializationBasedPayloadSerializerDeserializer;
import org.apache.hyracks.ipc.security.NetworkSecurityConfig;
import org.apache.hyracks.ipc.security.NetworkSecurityManager;
import org.apache.hyracks.util.ExitUtil;
import org.apache.hyracks.util.MaintainedThreadNameExecutorService;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xml.sax.InputSource;

public class ClusterControllerService implements IControllerService {
    private static final Logger LOGGER = LogManager.getLogger();

    private final CCConfig ccConfig;

    private final ConfigManager configManager;

    private IPCSystem clusterIPC;

    private IPCSystem clientIPC;

    private final LogFile jobLog;

    private ServerContext serverCtx;

    private WebServer webServer;

    private ClusterControllerInfo info;

    private CCServiceContext serviceCtx;

    private final DeployedJobSpecStore deployedJobSpecStore = new DeployedJobSpecStore();

    private final Map<JobId, JobParameterByteStore> jobParameterByteStoreMap = new HashMap<>();

    private final WorkQueue workQueue;

    private ExecutorService executor;

    private final Timer timer;

    private final ICCContext ccContext;

    private final DeadNodeSweeper sweeper;

    private final IResultDirectoryService resultDirectoryService;

    private final Map<DeploymentId, DeploymentRun> deploymentRunMap;

    private final Map<String, StateDumpRun> stateDumpRunMap;

    private final Map<String, ThreadDumpRun> threadDumpRunMap;

    private final INodeManager nodeManager;

    private final IResourceManager resourceManager = new ResourceManager();

    private final ICCApplication application;

    private final JobIdFactory jobIdFactory;

    private final DeployedJobSpecIdFactory deployedJobSpecIdFactory;

    private IJobManager jobManager;

    private ShutdownRun shutdownCallback;

    private final CcId ccId;

    private final INetworkSecurityManager networkSecurityManager;

    static {
        ExitUtil.init();
    }

    public ClusterControllerService(final CCConfig config) throws Exception {
        this(config, getApplication(config));
    }

    public ClusterControllerService(final CCConfig config, final ICCApplication application) throws Exception {
        this.ccConfig = config;
        this.configManager = ccConfig.getConfigManager();
        if (application == null) {
            throw new IllegalArgumentException("ICCApplication cannot be null");
        }
        this.application = application;
        File jobLogFolder = new File(ccConfig.getRootDir(), "logs/jobs");
        jobLog = new LogFile(jobLogFolder);

        final INetworkSecurityConfig securityConfig = getNetworkSecurityConfig();
        networkSecurityManager = new NetworkSecurityManager(securityConfig);

        // WorkQueue is in charge of heartbeat as well as other events.
        workQueue = new WorkQueue("ClusterController", Thread.MAX_PRIORITY);
        this.timer = new Timer(true);
        final ClusterTopology topology = computeClusterTopology(ccConfig);
        ccContext = new ClusterControllerContext(topology);
        sweeper = new DeadNodeSweeper();
        resultDirectoryService =
                new ResultDirectoryService(ccConfig.getResultTTL(), ccConfig.getResultSweepThreshold());

        deploymentRunMap = new HashMap<>();
        stateDumpRunMap = new HashMap<>();
        threadDumpRunMap = Collections.synchronizedMap(new HashMap<>());

        // Node manager is in charge of cluster membership management.
        nodeManager = new NodeManager(this, ccConfig, resourceManager, application.getGatekeeper());

        ccId = ccConfig.getCcId();
        jobIdFactory = new JobIdFactory(ccId);

        deployedJobSpecIdFactory = new DeployedJobSpecIdFactory();
    }

    private static ClusterTopology computeClusterTopology(CCConfig ccConfig) throws Exception {
        if (ccConfig.getClusterTopology() == null) {
            return null;
        }
        FileReader fr = new FileReader(ccConfig.getClusterTopology());
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
        serverCtx = new ServerContext(ServerContext.ServerType.CLUSTER_CONTROLLER, new File(ccConfig.getRootDir()));
        IIPCI ccIPCI = new ClusterControllerIPCI(this);
        clusterIPC = new IPCSystem(new InetSocketAddress(ccConfig.getClusterListenPort()),
                networkSecurityManager.getSocketChannelFactory(), ccIPCI, new CCNCFunctions.SerializerDeserializer());
        IIPCI ciIPCI = new ClientInterfaceIPCI(this, jobIdFactory);
        clientIPC =
                new IPCSystem(new InetSocketAddress(ccConfig.getClientListenAddress(), ccConfig.getClientListenPort()),
                        networkSecurityManager.getSocketChannelFactory(), ciIPCI,
                        new JavaSerializationBasedPayloadSerializerDeserializer());
        clusterIPC.start();
        clientIPC.start();
        startWebServers();
        info = new ClusterControllerInfo(ccId, ccConfig.getClientPublicAddress(), ccConfig.getClientPublicPort(),
                ccConfig.getConsolePublicPort());
        timer.schedule(sweeper, 0, ccConfig.getDeadNodeSweepThreshold());
        jobLog.open();
        startApplication();

        resultDirectoryService.init(executor, application.getJobResultCallback());
        workQueue.start();
        connectNCs();
        LOGGER.log(Level.INFO, "Started ClusterControllerService");
        notifyApplication();
    }

    protected void startWebServers() throws Exception {
        webServer = new WebServer(this, ccConfig.getConsoleListenPort());
        webServer.start();
    }

    private void startApplication() throws Exception {
        serviceCtx = new CCServiceContext(this, serverCtx, ccContext, ccConfig.getAppConfig());
        serviceCtx.addJobLifecycleListener(resultDirectoryService);
        application.init(serviceCtx);
        executor = MaintainedThreadNameExecutorService.newCachedThreadPool(serviceCtx.getThreadFactory());
        application.start(ccConfig.getAppArgsArray());
        IJobCapacityController jobCapacityController = application.getJobCapacityController();

        // Job manager is in charge of job lifecycle management.
        try {
            Constructor<?> jobManagerConstructor =
                    this.getClass().getClassLoader().loadClass(ccConfig.getJobManagerClass()).getConstructor(
                            CCConfig.class, ClusterControllerService.class, IJobCapacityController.class);
            jobManager = (IJobManager) jobManagerConstructor.newInstance(ccConfig, this, jobCapacityController);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException
                | InvocationTargetException e) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.log(Level.WARN, "class " + ccConfig.getJobManagerClass() + " could not be used: ", e);
            }
            // Falls back to the default implementation if the user-provided class name is not valid.
            jobManager = new JobManager(ccConfig, this, jobCapacityController);
        }
    }

    private InetSocketAddress getNCService(String nodeId) {
        IApplicationConfig ncConfig = configManager.getNodeEffectiveConfig(nodeId);
        final int port = ncConfig.getInt(NCConfig.Option.NCSERVICE_PORT);
        return port == NCConfig.NCSERVICE_PORT_DISABLED ? null
                : InetSocketAddress.createUnresolved(ncConfig.getString(NCConfig.Option.NCSERVICE_ADDRESS), port);
    }

    private Map<String, InetSocketAddress> getNCServices() {
        Map<String, InetSocketAddress> ncMap = new TreeMap<>();
        for (String ncId : configManager.getNodeNames()) {
            InetSocketAddress ncService = getNCService(ncId);
            if (ncService != null) {
                ncMap.put(ncId, ncService);
            }
        }
        return ncMap;
    }

    private void connectNCs() {
        getNCServices().forEach((key, value) -> {
            final TriggerNCWork triggerWork =
                    new TriggerNCWork(ClusterControllerService.this, value.getHostString(), value.getPort(), key);
            executor.submit(triggerWork);
        });
    }

    public boolean startNC(String nodeId) {
        InetSocketAddress ncServiceAddress = getNCService(nodeId);
        if (ncServiceAddress == null) {
            return false;
        }
        final TriggerNCWork startNc = new TriggerNCWork(ClusterControllerService.this, ncServiceAddress.getHostString(),
                ncServiceAddress.getPort(), nodeId);
        executor.submit(startNc);
        return true;

    }

    private void terminateNCServices() throws Exception {
        List<ShutdownNCServiceWork> shutdownNCServiceWorks = new ArrayList<>();
        getNCServices().forEach((key, value) -> {
            ShutdownNCServiceWork shutdownWork = new ShutdownNCServiceWork(value.getHostString(), value.getPort(), key);
            workQueue.schedule(shutdownWork);
            shutdownNCServiceWorks.add(shutdownWork);
        });
        for (ShutdownNCServiceWork shutdownWork : shutdownNCServiceWorks) {
            shutdownWork.sync();
        }
    }

    private void notifyApplication() throws Exception {
        application.startupCompleted();
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
        stopWebServers();
        sweeper.cancel();
        workQueue.stop();
        executor.shutdownNow();
        clusterIPC.stop();
        jobLog.close();
        clientIPC.stop();
        LOGGER.log(Level.INFO, "Stopped ClusterControllerService");
    }

    protected void stopWebServers() throws Exception {
        webServer.stop();
    }

    private void stopApplication() throws Exception {
        application.stop();
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

    public DeployedJobSpecStore getDeployedJobSpecStore() throws HyracksException {
        return deployedJobSpecStore;
    }

    public void removeJobParameterByteStore(JobId jobId) throws HyracksException {
        jobParameterByteStoreMap.remove(jobId);
    }

    public JobParameterByteStore createOrGetJobParameterByteStore(JobId jobId) throws HyracksException {
        JobParameterByteStore jpbs = jobParameterByteStoreMap.get(jobId);
        if (jpbs == null) {
            jpbs = new JobParameterByteStore();
            jobParameterByteStoreMap.put(jobId, jpbs);
        }
        return jpbs;
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

    @Override
    public ExecutorService getExecutor() {
        return executor;
    }

    @Override
    public Timer getTimer() {
        return timer;
    }

    public CCConfig getConfig() {
        return ccConfig;
    }

    @Override
    public CCServiceContext getContext() {
        return serviceCtx;
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

    public NetworkAddress getResultDirectoryAddress() {
        return new NetworkAddress(ccConfig.getClientPublicAddress(), ccConfig.getClientPublicPort());
    }

    public JobIdFactory getJobIdFactory() {
        return jobIdFactory;
    }

    public DeployedJobSpecIdFactory getDeployedJobSpecIdFactory() {
        return deployedJobSpecIdFactory;
    }

    public CcId getCcId() {
        return ccId;
    }

    private final class ClusterControllerContext implements ICCContext {
        private final ClusterTopology topology;

        private ClusterControllerContext(ClusterTopology topology) {
            this.topology = topology;
        }

        @Override
        public void getIPAddressNodeMap(Map<InetAddress, Set<String>> map) throws HyracksDataException {
            GetIpAddressNodeNameMapWork ginmw =
                    new GetIpAddressNodeNameMapWork(ClusterControllerService.this.getNodeManager(), map);
            try {
                workQueue.scheduleAndSync(ginmw);
            } catch (Exception e) {
                throw HyracksDataException.create(e);
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

    public IResultDirectoryService getResultDirectoryService() {
        return resultDirectoryService;
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

    private static ICCApplication getApplication(CCConfig ccConfig)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        if (ccConfig.getAppClass() != null) {
            Class<?> c = Class.forName(ccConfig.getAppClass());
            return (ICCApplication) c.newInstance();
        } else {
            return BaseCCApplication.INSTANCE;
        }
    }

    public ICCApplication getApplication() {
        return application;
    }

    @Override
    public Object getApplicationContext() {
        return application.getApplicationContext();
    }

    @Override
    public INetworkSecurityManager getNetworkSecurityManager() {
        return networkSecurityManager;
    }

    protected INetworkSecurityConfig getNetworkSecurityConfig() {
        return NetworkSecurityConfig.of(ccConfig.isSslEnabled(), ccConfig.getKeyStorePath(),
                ccConfig.getKeyStorePassword(), ccConfig.getTrustStorePath());
    }
}
