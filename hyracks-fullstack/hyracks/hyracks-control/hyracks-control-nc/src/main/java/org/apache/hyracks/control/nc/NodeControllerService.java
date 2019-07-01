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
package org.apache.hyracks.control.nc;

import static org.apache.hyracks.util.MXHelper.gcMXBeans;
import static org.apache.hyracks.util.MXHelper.memoryMXBean;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.api.application.INCApplication;
import org.apache.hyracks.api.client.NodeControllerInfo;
import org.apache.hyracks.api.client.NodeStatus;
import org.apache.hyracks.api.comm.NetworkAddress;
import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.deployment.DeploymentId;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.io.IODeviceHandle;
import org.apache.hyracks.api.job.ActivityClusterGraph;
import org.apache.hyracks.api.job.DeployedJobSpecId;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobParameterByteStore;
import org.apache.hyracks.api.lifecycle.ILifeCycleComponentManager;
import org.apache.hyracks.api.lifecycle.LifeCycleComponentManager;
import org.apache.hyracks.api.network.INetworkSecurityConfig;
import org.apache.hyracks.api.network.INetworkSecurityManager;
import org.apache.hyracks.api.result.IResultPartitionManager;
import org.apache.hyracks.api.service.IControllerService;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.control.common.NodeControllerData;
import org.apache.hyracks.control.common.base.IClusterController;
import org.apache.hyracks.control.common.config.ConfigManager;
import org.apache.hyracks.control.common.context.ServerContext;
import org.apache.hyracks.control.common.controllers.NCConfig;
import org.apache.hyracks.control.common.controllers.NodeParameters;
import org.apache.hyracks.control.common.controllers.NodeRegistration;
import org.apache.hyracks.control.common.heartbeat.HeartbeatSchema;
import org.apache.hyracks.control.common.ipc.CCNCFunctions;
import org.apache.hyracks.control.common.ipc.ClusterControllerRemoteProxy;
import org.apache.hyracks.control.common.job.profiling.om.JobProfile;
import org.apache.hyracks.control.common.work.FutureValue;
import org.apache.hyracks.control.common.work.WorkQueue;
import org.apache.hyracks.control.nc.application.NCServiceContext;
import org.apache.hyracks.control.nc.heartbeat.HeartbeatComputeTask;
import org.apache.hyracks.control.nc.heartbeat.HeartbeatManager;
import org.apache.hyracks.control.nc.io.IOManager;
import org.apache.hyracks.control.nc.net.MessagingNetworkManager;
import org.apache.hyracks.control.nc.net.NetworkManager;
import org.apache.hyracks.control.nc.net.ResultNetworkManager;
import org.apache.hyracks.control.nc.partitions.PartitionManager;
import org.apache.hyracks.control.nc.resources.memory.MemoryManager;
import org.apache.hyracks.control.nc.result.ResultPartitionManager;
import org.apache.hyracks.control.nc.work.AbortAllJobsWork;
import org.apache.hyracks.control.nc.work.BuildJobProfilesWork;
import org.apache.hyracks.ipc.api.IIPCEventListener;
import org.apache.hyracks.ipc.api.IIPCHandle;
import org.apache.hyracks.ipc.exceptions.IPCException;
import org.apache.hyracks.ipc.impl.IPCSystem;
import org.apache.hyracks.ipc.security.NetworkSecurityConfig;
import org.apache.hyracks.ipc.security.NetworkSecurityManager;
import org.apache.hyracks.net.protocols.muxdemux.FullFrameChannelInterfaceFactory;
import org.apache.hyracks.util.ExitUtil;
import org.apache.hyracks.util.MaintainedThreadNameExecutorService;
import org.apache.hyracks.util.trace.ITracer;
import org.apache.hyracks.util.trace.TraceUtils;
import org.apache.hyracks.util.trace.Tracer;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;

public class NodeControllerService implements IControllerService {
    private static final Logger LOGGER = LogManager.getLogger();

    private static final double MEMORY_FUDGE_FACTOR = 0.8;
    private static final int HEARTBEAT_REFRESH_MILLIS = 60000;

    private final NCConfig ncConfig;

    private final String id;

    private final IOManager ioManager;

    private IPCSystem ipc;

    private PartitionManager partitionManager;

    private NetworkManager netManager;

    private IResultPartitionManager resultPartitionManager;

    private ResultNetworkManager resultNetworkManager;

    private final WorkQueue workQueue;

    private final Timer timer;

    private CcId primaryCcId;

    private final Object ccLock = new Object();

    private final Map<CcId, CcConnection> ccMap = Collections.synchronizedMap(new HashMap<>());

    private final Map<InetSocketAddress, CcId> ccAddressMap = Collections.synchronizedMap(new HashMap<>());

    private final Map<Integer, CcConnection> pendingRegistrations = Collections.synchronizedMap(new HashMap<>());

    private final Map<JobId, Joblet> jobletMap;

    private final Map<Long, ActivityClusterGraph> deployedJobSpecActivityClusterGraphMap;

    private final Map<JobId, JobParameterByteStore> jobParameterByteStoreMap = new HashMap<>();

    private ExecutorService executor;

    private Map<CcId, HeartbeatManager> heartbeatManagers = new ConcurrentHashMap<>();

    private Map<CcId, Timer> ccTimers = new ConcurrentHashMap<>();

    private final ServerContext serverCtx;

    private NCServiceContext serviceCtx;

    private final INCApplication application;

    private final ILifeCycleComponentManager lccm;

    private final Mutable<FutureValue<Map<String, NodeControllerInfo>>> getNodeControllerInfosAcceptor;

    private final MemoryManager memoryManager;

    private final INetworkSecurityManager networkSecurityManager;

    private StackTraceElement[] shutdownCallStack;

    private MessagingNetworkManager messagingNetManager;

    private final ConfigManager configManager;

    private final Map<CcId, AtomicLong> maxJobIds = new ConcurrentHashMap<>();

    private volatile NodeStatus status = NodeStatus.ACTIVE;

    private NodeRegistration nodeRegistration;

    private NodeControllerData ncData;

    private HeartbeatComputeTask hbTask;

    private static final AtomicInteger nextRegistrationId = new AtomicInteger();

    static {
        ExitUtil.init();
    }

    private NCShutdownHook ncShutdownHook;

    public NodeControllerService(NCConfig config) throws Exception {
        this(config, getApplication(config));
    }

    public NodeControllerService(NCConfig config, INCApplication application) throws IOException, CmdLineException {
        ncConfig = config;
        configManager = ncConfig.getConfigManager();
        if (application == null) {
            throw new IllegalArgumentException("INCApplication cannot be null");
        }
        final INetworkSecurityConfig securityConfig = getNetworkSecurityConfig();
        networkSecurityManager = new NetworkSecurityManager(securityConfig);
        this.application = application;
        id = ncConfig.getNodeId();
        if (id == null) {
            throw new HyracksException("id not set");
        }
        lccm = new LifeCycleComponentManager();
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Setting uncaught exception handler " + getLifeCycleComponentManager());
        }
        // Set shutdown hook before so it doesn't have the same uncaught exception handler
        ncShutdownHook = new NCShutdownHook(this);
        Runtime.getRuntime().addShutdownHook(ncShutdownHook);
        Thread.currentThread().setUncaughtExceptionHandler(getLifeCycleComponentManager());
        ioManager = new IOManager(IODeviceHandle.getDevices(ncConfig.getIODevices()),
                application.getFileDeviceResolver(), ncConfig.getIOParallelism(), ncConfig.getIOQueueSize());
        try {
            workQueue = new WorkQueue(id, Thread.NORM_PRIORITY); // Reserves MAX_PRIORITY of the heartbeat thread.
            jobletMap = new ConcurrentHashMap<>();
            deployedJobSpecActivityClusterGraphMap = new Hashtable<>();
            timer = new Timer(true);
            serverCtx = new ServerContext(ServerContext.ServerType.NODE_CONTROLLER,
                    new File(ioManager.getWorkspacePath(0), id));
            getNodeControllerInfosAcceptor = new MutableObject<>();
            memoryManager =
                    new MemoryManager((long) (memoryMXBean.getHeapMemoryUsage().getMax() * MEMORY_FUDGE_FACTOR));
        } catch (Throwable th) { // NOSONAR will be re-thrown
            CleanupUtils.close(ioManager, th);
            throw th;
        }
    }

    public IOManager getIoManager() {
        return ioManager;
    }

    @Override
    public NCServiceContext getContext() {
        return serviceCtx;
    }

    public ILifeCycleComponentManager getLifeCycleComponentManager() {
        return lccm;
    }

    public Map<String, NodeControllerInfo> getNodeControllersInfo() throws Exception {
        FutureValue<Map<String, NodeControllerInfo>> fv = new FutureValue<>();
        synchronized (getNodeControllerInfosAcceptor) {
            while (getNodeControllerInfosAcceptor.getValue() != null) {
                getNodeControllerInfosAcceptor.wait();
            }
            getNodeControllerInfosAcceptor.setValue(fv);
        }
        getPrimaryClusterController().getNodeControllerInfos();
        return fv.get();
    }

    void setNodeControllersInfo(Map<String, NodeControllerInfo> ncInfos) {
        FutureValue<Map<String, NodeControllerInfo>> fv;
        synchronized (getNodeControllerInfosAcceptor) {
            fv = getNodeControllerInfosAcceptor.getValue();
            getNodeControllerInfosAcceptor.setValue(null);
            getNodeControllerInfosAcceptor.notifyAll();
        }
        fv.setValue(ncInfos);
    }

    private void init() {
        resultPartitionManager = new ResultPartitionManager(this, executor, ncConfig.getResultManagerMemory(),
                ncConfig.getResultTTL(), ncConfig.getResultSweepThreshold());
        resultNetworkManager = new ResultNetworkManager(ncConfig.getResultListenAddress(),
                ncConfig.getResultListenPort(), resultPartitionManager, ncConfig.getNetThreadCount(),
                ncConfig.getNetBufferCount(), ncConfig.getResultPublicAddress(), ncConfig.getResultPublicPort(),
                FullFrameChannelInterfaceFactory.INSTANCE, networkSecurityManager.getSocketChannelFactory());
        if (ncConfig.getMessagingListenAddress() != null && serviceCtx.getMessagingChannelInterfaceFactory() != null) {
            messagingNetManager = new MessagingNetworkManager(this, ncConfig.getMessagingListenAddress(),
                    ncConfig.getMessagingListenPort(), ncConfig.getNetThreadCount(),
                    ncConfig.getMessagingPublicAddress(), ncConfig.getMessagingPublicPort(),
                    serviceCtx.getMessagingChannelInterfaceFactory(), networkSecurityManager.getSocketChannelFactory());
        }
    }

    @Override
    public void start() throws Exception {
        LOGGER.log(Level.INFO, "Starting NodeControllerService");
        ipc = new IPCSystem(new InetSocketAddress(ncConfig.getClusterListenAddress(), ncConfig.getClusterListenPort()),
                networkSecurityManager.getSocketChannelFactory(), new NodeControllerIPCI(this),
                new CCNCFunctions.SerializerDeserializer());
        ipc.start();
        partitionManager = new PartitionManager(this);
        netManager = new NetworkManager(ncConfig.getDataListenAddress(), ncConfig.getDataListenPort(), partitionManager,
                ncConfig.getNetThreadCount(), ncConfig.getNetBufferCount(), ncConfig.getDataPublicAddress(),
                ncConfig.getDataPublicPort(), FullFrameChannelInterfaceFactory.INSTANCE,
                networkSecurityManager.getSocketChannelFactory());
        netManager.start();
        startApplication();
        init();
        resultNetworkManager.start();
        if (messagingNetManager != null) {
            messagingNetManager.start();
        }
        initNodeControllerState();
        hbTask = new HeartbeatComputeTask(this);
        primaryCcId = addCc(new InetSocketAddress(ncConfig.getClusterAddress(), ncConfig.getClusterPort()));

        workQueue.start();

        // Schedule heartbeat data updates
        timer.schedule(hbTask, HEARTBEAT_REFRESH_MILLIS, HEARTBEAT_REFRESH_MILLIS);

        // Schedule tracing a human-readable datetime
        timer.schedule(new TraceCurrentTimeTask(serviceCtx.getTracer()), 0, 60000);

        LOGGER.log(Level.INFO, "Started NodeControllerService");
        application.startupCompleted();
    }

    private void initNodeControllerState() {
        // Use "public" versions of network addresses and ports, if defined
        InetSocketAddress ncAddress;
        if (ncConfig.getClusterPublicPort() == 0) {
            ncAddress = ipc.getSocketAddress();
        } else {
            ncAddress = new InetSocketAddress(ncConfig.getClusterPublicAddress(), ncConfig.getClusterPublicPort());
        }
        HeartbeatSchema.GarbageCollectorInfo[] gcInfos = new HeartbeatSchema.GarbageCollectorInfo[gcMXBeans.size()];
        for (int i = 0; i < gcInfos.length; ++i) {
            gcInfos[i] = new HeartbeatSchema.GarbageCollectorInfo(gcMXBeans.get(i).getName());
        }
        HeartbeatSchema hbSchema = new HeartbeatSchema(gcInfos);

        NetworkAddress resultAddress = resultNetworkManager.getPublicNetworkAddress();
        NetworkAddress netAddress = netManager.getPublicNetworkAddress();
        NetworkAddress messagingAddress =
                messagingNetManager != null ? messagingNetManager.getPublicNetworkAddress() : null;
        nodeRegistration = new NodeRegistration(ncAddress, id, ncConfig, netAddress, resultAddress, hbSchema,
                messagingAddress, application.getCapacity());

        ncData = new NodeControllerData(nodeRegistration);
    }

    public CcId addCc(InetSocketAddress ccAddress) throws Exception {
        synchronized (ccLock) {
            LOGGER.info("addCc: {}", ccAddress);
            if (ccAddress.isUnresolved()) {
                throw new IllegalArgumentException("must use resolved InetSocketAddress");
            }
            if (ccAddressMap.containsKey(ccAddress)) {
                throw new IllegalStateException("cc already registered: " + ccAddress);
            }
            final IIPCEventListener ipcEventListener = new IIPCEventListener() {
                @Override
                public void ipcHandleRestored(IIPCHandle handle) throws IPCException {
                    // we need to re-register in case of NC -> CC connection reset
                    final CcConnection ccConnection = getCcConnection(ccAddressMap.get(ccAddress));
                    try {
                        ccConnection.forceReregister(NodeControllerService.this);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IPCException(e);
                    }
                }
            };
            ClusterControllerRemoteProxy ccProxy = new ClusterControllerRemoteProxy(
                    ipc.getHandle(ccAddress, ncConfig.getClusterConnectRetries(), 1, ipcEventListener));
            return registerNode(new CcConnection(ccProxy, ccAddress));
        }
    }

    public void makePrimaryCc(InetSocketAddress ccAddress) {
        LOGGER.info("makePrimaryCc: {}", ccAddress);
        if (ccAddress.isUnresolved()) {
            throw new IllegalArgumentException("must use resolved InetSocketAddress");
        }
        CcId newPrimaryCc = ccAddressMap.get(ccAddress);
        if (newPrimaryCc == null) {
            throw new IllegalArgumentException("unknown cc: " + ccAddress);
        }
        this.primaryCcId = newPrimaryCc;
    }

    public void removeCc(InetSocketAddress ccAddress) throws Exception {
        synchronized (ccLock) {
            LOGGER.info("removeCc: {}", ccAddress);
            if (ccAddress.isUnresolved()) {
                throw new IllegalArgumentException("must use resolved InetSocketAddress");
            }
            CcId ccId = ccAddressMap.get(ccAddress);
            if (ccId == null) {
                LOGGER.warn("ignoring request to remove unknown cc: {}", ccAddress);
                return;
            }
            if (primaryCcId.equals(ccId)) {
                throw new IllegalStateException("cannot remove primary cc: " + ccAddress);
            }
            try {
                final CcConnection ccc = getCcConnection(ccId);
                ccc.getClusterControllerService().unregisterNode(id);
            } catch (Exception e) {
                LOGGER.warn("ignoring exception trying to gracefully unregister cc {}: ", () -> ccId,
                        () -> String.valueOf(e));
            }
            getWorkQueue().scheduleAndSync(new AbortAllJobsWork(this, ccId));
            HeartbeatManager hbMgr = heartbeatManagers.remove(ccId);
            if (hbMgr != null) {
                hbMgr.shutdown();
            }
            Timer ccTimer = ccTimers.remove(ccId);
            if (ccTimer != null) {
                ccTimer.cancel();
            }
            ccMap.remove(ccId);
            ccAddressMap.remove(ccAddress);
        }
    }

    public CcId registerNode(CcConnection ccc) throws Exception {
        LOGGER.info("Registering with Cluster Controller {}", ccc);
        int registrationId = nextRegistrationId.incrementAndGet();
        pendingRegistrations.put(registrationId, ccc);
        CcId ccId = ccc.registerNode(nodeRegistration, registrationId);
        ccMap.put(ccId, ccc);
        ccAddressMap.put(ccc.getCcAddress(), ccId);
        Serializable distributedState = ccc.getNodeParameters().getDistributedState();
        if (distributedState != null) {
            getDistributedState().put(ccId, distributedState);
        }
        IClusterController ccs = ccc.getClusterControllerService();
        NodeParameters nodeParameters = ccc.getNodeParameters();
        // Start heartbeat generator.
        heartbeatManagers.computeIfAbsent(ccId, newCcId -> HeartbeatManager.init(this, ccc, hbTask.getHeartbeatData(),
                nodeRegistration.getNodeControllerAddress()));
        if (!ccTimers.containsKey(ccId) && nodeParameters.getProfileDumpPeriod() > 0) {
            Timer ccTimer = new Timer("Timer-" + ccId, true);
            // Schedule profile dump generator.
            ccTimer.schedule(new ProfileDumpTask(ccs, ccId), 0, nodeParameters.getProfileDumpPeriod());
            ccTimers.put(ccId, ccTimer);
        }
        ccc.notifyRegistrationCompleted();
        LOGGER.info("Registering with Cluster Controller {} completed", ccc);
        return ccId;
    }

    void setNodeRegistrationResult(NodeParameters parameters, Exception exception) {
        CcConnection ccc = getPendingNodeRegistration(parameters);
        ccc.setNodeRegistrationResult(parameters, exception);
    }

    private CcConnection getCcConnection(CcId ccId) {
        CcConnection ccConnection = ccMap.get(ccId);
        if (ccConnection == null) {
            throw new IllegalArgumentException("unknown ccId: " + ccId);
        }
        return ccConnection;
    }

    private CcConnection getPendingNodeRegistration(NodeParameters nodeParameters) {
        CcConnection ccConnection = pendingRegistrations.remove(nodeParameters.getRegistrationId());
        if (ccConnection == null) {
            throw new IllegalStateException("Unknown pending node registration " + nodeParameters.getRegistrationId()
                    + " for " + nodeParameters.getClusterControllerInfo().getCcId());
        }
        return ccConnection;
    }

    private ConcurrentHashMap<CcId, Serializable> getDistributedState() {
        return (ConcurrentHashMap<CcId, Serializable>) serviceCtx.getDistributedState();
    }

    private void startApplication() throws Exception {
        serviceCtx = new NCServiceContext(this, serverCtx, ioManager, id, memoryManager, lccm,
                ncConfig.getNodeScopedAppConfig());
        application.init(serviceCtx);
        executor = MaintainedThreadNameExecutorService.newCachedThreadPool(serviceCtx.getThreadFactory());
        application.start(ncConfig.getAppArgsArray());
    }

    public void updateMaxJobId(JobId jobId) {
        maxJobIds.computeIfAbsent(jobId.getCcId(), key -> new AtomicLong())
                .getAndUpdate(currentMaxId -> Math.max(currentMaxId, jobId.getId()));
    }

    public long getMaxJobId(CcId ccId) {
        return maxJobIds.computeIfAbsent(ccId, key -> new AtomicLong(ccId.toLongMask())).get();
    }

    @Override
    public synchronized void stop() throws Exception {
        if (shutdownCallStack == null) {
            shutdownCallStack = new Throwable().getStackTrace();
            LOGGER.log(Level.INFO, "Stopping NodeControllerService");
            application.preStop();
            executor.shutdownNow();
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                LOGGER.log(Level.ERROR, "Some jobs failed to exit, continuing with abnormal shutdown");
            }
            partitionManager.close();
            resultPartitionManager.close();
            netManager.stop();
            resultNetworkManager.stop();
            if (messagingNetManager != null) {
                messagingNetManager.stop();
            }
            workQueue.stop();
            application.stop();
            /*
             * Stop heartbeats only after NC has stopped to avoid false node failure detection
             * on CC if an NC takes a long time to stop.
             */
            heartbeatManagers.values().parallelStream().forEach(HeartbeatManager::shutdown);
            synchronized (ccLock) {
                ccMap.values().parallelStream().forEach(cc -> {
                    try {
                        cc.getClusterControllerService().notifyShutdown(id);
                    } catch (Exception e) {
                        LOGGER.log(Level.WARN, "Exception notifying CC of shutdown", e);
                    }
                });
            }
            ipc.stop();
            ioManager.close();
            LOGGER.log(Level.INFO, "Stopped NodeControllerService");
        } else {
            LOGGER.log(Level.ERROR, "Duplicate shutdown call; original: " + Arrays.toString(shutdownCallStack),
                    new Exception("Duplicate shutdown call"));
        }
        if (ncShutdownHook != null) {
            try {
                Runtime.getRuntime().removeShutdownHook(ncShutdownHook);
                LOGGER.info("removed shutdown hook for {}", id);
            } catch (IllegalStateException e) {
                LOGGER.log(Level.DEBUG, "ignoring exception while attempting to remove shutdown hook", e);
            }
        }
    }

    public String getId() {
        return id;
    }

    public ServerContext getServerContext() {
        return serverCtx;
    }

    public Map<JobId, Joblet> getJobletMap() {
        return jobletMap;
    }

    public void removeJobParameterByteStore(JobId jobId) {
        jobParameterByteStoreMap.remove(jobId);
    }

    public JobParameterByteStore createOrGetJobParameterByteStore(JobId jobId) {
        return jobParameterByteStoreMap.computeIfAbsent(jobId, jid -> new JobParameterByteStore());
    }

    public void storeActivityClusterGraph(DeployedJobSpecId deployedJobSpecId, ActivityClusterGraph acg)
            throws HyracksException {
        deployedJobSpecActivityClusterGraphMap.put(deployedJobSpecId.getId(), acg);
    }

    public void removeActivityClusterGraph(DeployedJobSpecId deployedJobSpecId) throws HyracksException {
        if (deployedJobSpecActivityClusterGraphMap.get(deployedJobSpecId.getId()) == null) {
            throw HyracksException.create(ErrorCode.ERROR_FINDING_DEPLOYED_JOB, deployedJobSpecId);
        }
        deployedJobSpecActivityClusterGraphMap.remove(deployedJobSpecId.getId());
    }

    public void checkForDuplicateDeployedJobSpec(DeployedJobSpecId deployedJobSpecId) throws HyracksException {
        if (deployedJobSpecActivityClusterGraphMap.get(deployedJobSpecId.getId()) != null) {
            throw HyracksException.create(ErrorCode.DUPLICATE_DEPLOYED_JOB, deployedJobSpecId);
        }
    }

    public ActivityClusterGraph getActivityClusterGraph(DeployedJobSpecId deployedJobSpecId) {
        return deployedJobSpecActivityClusterGraphMap.get(deployedJobSpecId.getId());
    }

    public NetworkManager getNetworkManager() {
        return netManager;
    }

    public ResultNetworkManager getResultNetworkManager() {
        return resultNetworkManager;
    }

    public PartitionManager getPartitionManager() {
        return partitionManager;
    }

    public CcId getPrimaryCcId() {
        // TODO(mblow): this can change at any time, need notification framework
        return primaryCcId;
    }

    public IClusterController getPrimaryClusterController() {
        return getClusterController(primaryCcId);
    }

    public IClusterController getClusterController(CcId ccId) {
        return getCcConnection(ccId).getClusterControllerService();
    }

    public NodeParameters getNodeParameters(CcId ccId) {
        return getCcConnection(ccId).getNodeParameters();
    }

    @Override
    public ExecutorService getExecutor() {
        return executor;
    }

    @Override
    public Timer getTimer() {
        return timer;
    }

    public NCConfig getConfiguration() {
        return ncConfig;
    }

    public WorkQueue getWorkQueue() {
        return workQueue;
    }

    public NodeStatus getNodeStatus() {
        return status;
    }

    public void setNodeStatus(NodeStatus status) {
        this.status = status;
    }

    public NodeControllerData getNodeControllerData() {
        return ncData;
    }

    public IPCSystem getIpcSystem() {
        return ipc;
    }

    public void sendApplicationMessageToCC(CcId ccId, byte[] data, DeploymentId deploymentId) throws Exception {
        getClusterController(ccId).sendApplicationMessageToCC(data, deploymentId, id);
    }

    public IResultPartitionManager getResultPartitionManager() {
        return resultPartitionManager;
    }

    public MessagingNetworkManager getMessagingNetworkManager() {
        return messagingNetManager;
    }

    public void notifyTasksCompleted(CcId ccId) throws Exception {
        partitionManager.jobsCompleted(ccId);
        application.tasksCompleted(ccId);
    }

    private static INCApplication getApplication(NCConfig config)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        if (config.getAppClass() != null) {
            Class<?> c = Class.forName(config.getAppClass());
            return (INCApplication) c.newInstance();
        } else {
            return BaseNCApplication.INSTANCE;
        }
    }

    @Override
    public Object getApplicationContext() {
        return application.getApplicationContext();
    }

    public HeartbeatManager getHeartbeatManager(CcId ccId) {
        return heartbeatManagers.get(ccId);
    }

    public NodeRegistration getNodeRegistration() {
        return nodeRegistration;
    }

    private class ProfileDumpTask extends TimerTask {
        private final IClusterController cc;
        private final CcId ccId;

        public ProfileDumpTask(IClusterController cc, CcId ccId) {
            this.cc = cc;
            this.ccId = ccId;
        }

        @Override
        public void run() {
            try {
                FutureValue<List<JobProfile>> fv = new FutureValue<>();
                BuildJobProfilesWork bjpw = new BuildJobProfilesWork(NodeControllerService.this, ccId, fv);
                workQueue.scheduleAndSync(bjpw);
                List<JobProfile> profiles = fv.get();
                if (!profiles.isEmpty()) {
                    cc.reportProfile(id, profiles);
                }
            } catch (Exception e) {
                LOGGER.log(Level.WARN, "Exception reporting profile", e);
            }
        }
    }

    private class TraceCurrentTimeTask extends TimerTask {
        private ITracer tracer;
        private long traceCategory;

        public TraceCurrentTimeTask(ITracer tracer) {
            this.tracer = tracer;
            this.traceCategory = tracer.getRegistry().get(TraceUtils.TIMESTAMP);
        }

        @Override
        public void run() {
            try {
                tracer.instant("CurrentTime", traceCategory, Tracer.Scope.p, Tracer.dateTimeStamp());
            } catch (Exception e) {
                LOGGER.log(Level.WARN, "Exception tracing current time", e);
            }
        }
    }

    public INCApplication getApplication() {
        return application;
    }

    @Override
    public INetworkSecurityManager getNetworkSecurityManager() {
        return networkSecurityManager;
    }

    protected INetworkSecurityConfig getNetworkSecurityConfig() {
        return NetworkSecurityConfig.of(ncConfig.isSslEnabled(), ncConfig.getKeyStorePath(),
                ncConfig.getKeyStorePassword(), ncConfig.getTrustStorePath());
    }
}
