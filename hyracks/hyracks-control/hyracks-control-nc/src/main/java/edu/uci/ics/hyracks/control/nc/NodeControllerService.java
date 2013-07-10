/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.control.nc;

import java.io.File;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;

import edu.uci.ics.hyracks.api.application.INCApplicationEntryPoint;
import edu.uci.ics.hyracks.api.client.NodeControllerInfo;
import edu.uci.ics.hyracks.api.context.IHyracksRootContext;
import edu.uci.ics.hyracks.api.dataset.IDatasetPartitionManager;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.control.common.AbstractRemoteService;
import edu.uci.ics.hyracks.control.common.base.IClusterController;
import edu.uci.ics.hyracks.control.common.context.ServerContext;
import edu.uci.ics.hyracks.control.common.controllers.NCConfig;
import edu.uci.ics.hyracks.control.common.controllers.NodeParameters;
import edu.uci.ics.hyracks.control.common.controllers.NodeRegistration;
import edu.uci.ics.hyracks.control.common.heartbeat.HeartbeatData;
import edu.uci.ics.hyracks.control.common.heartbeat.HeartbeatSchema;
import edu.uci.ics.hyracks.control.common.ipc.CCNCFunctions;
import edu.uci.ics.hyracks.control.common.ipc.ClusterControllerRemoteProxy;
import edu.uci.ics.hyracks.control.common.job.profiling.om.JobProfile;
import edu.uci.ics.hyracks.control.common.work.FutureValue;
import edu.uci.ics.hyracks.control.common.work.WorkQueue;
import edu.uci.ics.hyracks.control.nc.application.NCApplicationContext;
import edu.uci.ics.hyracks.control.nc.dataset.DatasetPartitionManager;
import edu.uci.ics.hyracks.control.nc.io.IOManager;
import edu.uci.ics.hyracks.control.nc.net.DatasetNetworkManager;
import edu.uci.ics.hyracks.control.nc.net.NetworkManager;
import edu.uci.ics.hyracks.control.nc.partitions.PartitionManager;
import edu.uci.ics.hyracks.control.nc.resources.memory.MemoryManager;
import edu.uci.ics.hyracks.control.nc.runtime.RootHyracksContext;
import edu.uci.ics.hyracks.control.nc.work.AbortTasksWork;
import edu.uci.ics.hyracks.control.nc.work.ApplicationMessageWork;
import edu.uci.ics.hyracks.control.nc.work.BuildJobProfilesWork;
import edu.uci.ics.hyracks.control.nc.work.CleanupJobletWork;
import edu.uci.ics.hyracks.control.nc.work.DeployBinaryWork;
import edu.uci.ics.hyracks.control.nc.work.ReportPartitionAvailabilityWork;
import edu.uci.ics.hyracks.control.nc.work.StartTasksWork;
import edu.uci.ics.hyracks.control.nc.work.UnDeployBinaryWork;
import edu.uci.ics.hyracks.ipc.api.IIPCHandle;
import edu.uci.ics.hyracks.ipc.api.IIPCI;
import edu.uci.ics.hyracks.ipc.api.IPCPerformanceCounters;
import edu.uci.ics.hyracks.ipc.impl.IPCSystem;
import edu.uci.ics.hyracks.net.protocols.muxdemux.MuxDemuxPerformanceCounters;

public class NodeControllerService extends AbstractRemoteService {
    private static Logger LOGGER = Logger.getLogger(NodeControllerService.class.getName());

    private static final double MEMORY_FUDGE_FACTOR = 0.8;

    private NCConfig ncConfig;

    private final String id;

    private final IHyracksRootContext ctx;

    private final IPCSystem ipc;

    private final PartitionManager partitionManager;

    private final NetworkManager netManager;

    private IDatasetPartitionManager datasetPartitionManager;

    private DatasetNetworkManager datasetNetworkManager;

    private final WorkQueue queue;

    private final Timer timer;

    private boolean registrationPending;

    private Exception registrationException;

    private IClusterController ccs;

    private final Map<JobId, Joblet> jobletMap;

    private ExecutorService executor;

    private NodeParameters nodeParameters;

    private HeartbeatTask heartbeatTask;

    private final ServerContext serverCtx;

    private NCApplicationContext appCtx;

    private INCApplicationEntryPoint ncAppEntryPoint;

    private final MemoryMXBean memoryMXBean;

    private final List<GarbageCollectorMXBean> gcMXBeans;

    private final ThreadMXBean threadMXBean;

    private final RuntimeMXBean runtimeMXBean;

    private final OperatingSystemMXBean osMXBean;

    private final Mutable<FutureValue<Map<String, NodeControllerInfo>>> getNodeControllerInfosAcceptor;

    private final MemoryManager memoryManager;

    public NodeControllerService(NCConfig ncConfig) throws Exception {
        this.ncConfig = ncConfig;
        id = ncConfig.nodeId;
        NodeControllerIPCI ipci = new NodeControllerIPCI();
        ipc = new IPCSystem(new InetSocketAddress(ncConfig.clusterNetIPAddress, 0), ipci,
                new CCNCFunctions.SerializerDeserializer());

        this.ctx = new RootHyracksContext(this, new IOManager(getDevices(ncConfig.ioDevices)));
        if (id == null) {
            throw new Exception("id not set");
        }
        partitionManager = new PartitionManager(this);
        netManager = new NetworkManager(getIpAddress(ncConfig.dataIPAddress), partitionManager, ncConfig.nNetThreads);

        queue = new WorkQueue();
        jobletMap = new Hashtable<JobId, Joblet>();
        timer = new Timer(true);
        serverCtx = new ServerContext(ServerContext.ServerType.NODE_CONTROLLER, new File(new File(
                NodeControllerService.class.getName()), id));
        memoryMXBean = ManagementFactory.getMemoryMXBean();
        gcMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
        threadMXBean = ManagementFactory.getThreadMXBean();
        runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        osMXBean = ManagementFactory.getOperatingSystemMXBean();
        registrationPending = true;
        getNodeControllerInfosAcceptor = new MutableObject<FutureValue<Map<String, NodeControllerInfo>>>();
        memoryManager = new MemoryManager((long) (memoryMXBean.getHeapMemoryUsage().getMax() * MEMORY_FUDGE_FACTOR));
    }

    public IHyracksRootContext getRootContext() {
        return ctx;
    }

    public NCApplicationContext getApplicationContext() {
        return appCtx;
    }

    private static List<IODeviceHandle> getDevices(String ioDevices) {
        List<IODeviceHandle> devices = new ArrayList<IODeviceHandle>();
        StringTokenizer tok = new StringTokenizer(ioDevices, ",");
        while (tok.hasMoreElements()) {
            String devPath = tok.nextToken().trim();
            devices.add(new IODeviceHandle(new File(devPath), "."));
        }
        return devices;
    }

    private synchronized void setNodeRegistrationResult(NodeParameters parameters, Exception exception) {
        this.nodeParameters = parameters;
        this.registrationException = exception;
        this.registrationPending = false;
        notifyAll();
    }

    public Map<String, NodeControllerInfo> getNodeControllersInfo() throws Exception {
        FutureValue<Map<String, NodeControllerInfo>> fv = new FutureValue<Map<String, NodeControllerInfo>>();
        synchronized (getNodeControllerInfosAcceptor) {
            while (getNodeControllerInfosAcceptor.getValue() != null) {
                getNodeControllerInfosAcceptor.wait();
            }
            getNodeControllerInfosAcceptor.setValue(fv);
        }
        ccs.getNodeControllerInfos();
        return fv.get();
    }

    private void setNodeControllersInfo(Map<String, NodeControllerInfo> ncInfos) {
        FutureValue<Map<String, NodeControllerInfo>> fv;
        synchronized (getNodeControllerInfosAcceptor) {
            fv = getNodeControllerInfosAcceptor.getValue();
            getNodeControllerInfosAcceptor.setValue(null);
            getNodeControllerInfosAcceptor.notifyAll();
        }
        fv.setValue(ncInfos);
    }

    private void init() throws Exception {
        ctx.getIOManager().setExecutor(executor);
        datasetPartitionManager = new DatasetPartitionManager(this, executor, ncConfig.resultManagerMemory,
                ncConfig.resultTTL, ncConfig.resultSweepThreshold);
        datasetNetworkManager = new DatasetNetworkManager(getIpAddress(ncConfig.datasetIPAddress),
                datasetPartitionManager, ncConfig.nNetThreads);
    }

    @Override
    public void start() throws Exception {
        LOGGER.log(Level.INFO, "Starting NodeControllerService");
        ipc.start();
        netManager.start();

        startApplication();
        init();

        datasetNetworkManager.start();
        IIPCHandle ccIPCHandle = ipc.getHandle(new InetSocketAddress(ncConfig.ccHost, ncConfig.ccPort));
        this.ccs = new ClusterControllerRemoteProxy(ccIPCHandle);
        HeartbeatSchema.GarbageCollectorInfo[] gcInfos = new HeartbeatSchema.GarbageCollectorInfo[gcMXBeans.size()];
        for (int i = 0; i < gcInfos.length; ++i) {
            gcInfos[i] = new HeartbeatSchema.GarbageCollectorInfo(gcMXBeans.get(i).getName());
        }
        HeartbeatSchema hbSchema = new HeartbeatSchema(gcInfos);
        ccs.registerNode(new NodeRegistration(ipc.getSocketAddress(), id, ncConfig, netManager.getNetworkAddress(),
                datasetNetworkManager.getNetworkAddress(), osMXBean.getName(), osMXBean.getArch(), osMXBean
                        .getVersion(), osMXBean.getAvailableProcessors(), runtimeMXBean.getVmName(), runtimeMXBean
                        .getVmVersion(), runtimeMXBean.getVmVendor(), runtimeMXBean.getClassPath(), runtimeMXBean
                        .getLibraryPath(), runtimeMXBean.getBootClassPath(), runtimeMXBean.getInputArguments(),
                runtimeMXBean.getSystemProperties(), hbSchema));

        synchronized (this) {
            while (registrationPending) {
                wait();
            }
        }
        if (registrationException != null) {
            throw registrationException;
        }
        appCtx.setDistributedState(nodeParameters.getDistributedState());

        queue.start();

        heartbeatTask = new HeartbeatTask(ccs);

        // Schedule heartbeat generator.
        timer.schedule(heartbeatTask, 0, nodeParameters.getHeartbeatPeriod());

        if (nodeParameters.getProfileDumpPeriod() > 0) {
            // Schedule profile dump generator.
            timer.schedule(new ProfileDumpTask(ccs), 0, nodeParameters.getProfileDumpPeriod());
        }

        LOGGER.log(Level.INFO, "Started NodeControllerService");
        if (ncAppEntryPoint != null) {
            ncAppEntryPoint.notifyStartupComplete();
        }
    }

    private void startApplication() throws Exception {
        appCtx = new NCApplicationContext(serverCtx, ctx, id, memoryManager);
        String className = ncConfig.appNCMainClass;
        if (className != null) {
            Class<?> c = Class.forName(className);
            ncAppEntryPoint = (INCApplicationEntryPoint) c.newInstance();
            String[] args = ncConfig.appArgs == null ? new String[0] : ncConfig.appArgs
                    .toArray(new String[ncConfig.appArgs.size()]);
            ncAppEntryPoint.start(appCtx, args);
        }
        executor = Executors.newCachedThreadPool(appCtx.getThreadFactory());
    }

    @Override
    public void stop() throws Exception {
        LOGGER.log(Level.INFO, "Stopping NodeControllerService");
        executor.shutdownNow();
        partitionManager.close();
        datasetPartitionManager.close();
        heartbeatTask.cancel();
        netManager.stop();
        datasetNetworkManager.stop();
        queue.stop();
        LOGGER.log(Level.INFO, "Stopped NodeControllerService");
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

    public NetworkManager getNetworkManager() {
        return netManager;
    }

    public DatasetNetworkManager getDatasetNetworkManager() {
        return datasetNetworkManager;
    }

    public PartitionManager getPartitionManager() {
        return partitionManager;
    }

    public IClusterController getClusterController() {
        return ccs;
    }

    public NodeParameters getNodeParameters() {
        return nodeParameters;
    }

    public Executor getExecutor() {
        return executor;
    }

    public NCConfig getConfiguration() {
        return ncConfig;
    }

    public WorkQueue getWorkQueue() {
        return queue;
    }

    private static InetAddress getIpAddress(String ipaddrStr) throws Exception {
        ipaddrStr = ipaddrStr.trim();
        Pattern pattern = Pattern.compile("(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})");
        Matcher m = pattern.matcher(ipaddrStr);
        if (!m.matches()) {
            throw new Exception(MessageFormat.format(
                    "Connection Manager IP Address String %s does is not a valid IP Address.", ipaddrStr));
        }
        byte[] ipBytes = new byte[4];
        ipBytes[0] = (byte) Integer.parseInt(m.group(1));
        ipBytes[1] = (byte) Integer.parseInt(m.group(2));
        ipBytes[2] = (byte) Integer.parseInt(m.group(3));
        ipBytes[3] = (byte) Integer.parseInt(m.group(4));
        return InetAddress.getByAddress(ipBytes);
    }

    private class HeartbeatTask extends TimerTask {
        private IClusterController cc;

        private final HeartbeatData hbData;

        public HeartbeatTask(IClusterController cc) {
            this.cc = cc;
            hbData = new HeartbeatData();
            hbData.gcCollectionCounts = new long[gcMXBeans.size()];
            hbData.gcCollectionTimes = new long[gcMXBeans.size()];
        }

        @Override
        public void run() {
            MemoryUsage heapUsage = memoryMXBean.getHeapMemoryUsage();
            hbData.heapInitSize = heapUsage.getInit();
            hbData.heapUsedSize = heapUsage.getUsed();
            hbData.heapCommittedSize = heapUsage.getCommitted();
            hbData.heapMaxSize = heapUsage.getMax();
            MemoryUsage nonheapUsage = memoryMXBean.getNonHeapMemoryUsage();
            hbData.nonheapInitSize = nonheapUsage.getInit();
            hbData.nonheapUsedSize = nonheapUsage.getUsed();
            hbData.nonheapCommittedSize = nonheapUsage.getCommitted();
            hbData.nonheapMaxSize = nonheapUsage.getMax();
            hbData.threadCount = threadMXBean.getThreadCount();
            hbData.peakThreadCount = threadMXBean.getPeakThreadCount();
            hbData.totalStartedThreadCount = threadMXBean.getTotalStartedThreadCount();
            hbData.systemLoadAverage = osMXBean.getSystemLoadAverage();
            int gcN = gcMXBeans.size();
            for (int i = 0; i < gcN; ++i) {
                GarbageCollectorMXBean gcMXBean = gcMXBeans.get(i);
                hbData.gcCollectionCounts[i] = gcMXBean.getCollectionCount();
                hbData.gcCollectionTimes[i] = gcMXBean.getCollectionTime();
            }

            MuxDemuxPerformanceCounters netPC = netManager.getPerformanceCounters();
            hbData.netPayloadBytesRead = netPC.getPayloadBytesRead();
            hbData.netPayloadBytesWritten = netPC.getPayloadBytesWritten();
            hbData.netSignalingBytesRead = netPC.getSignalingBytesRead();
            hbData.netSignalingBytesWritten = netPC.getSignalingBytesWritten();

            MuxDemuxPerformanceCounters datasetNetPC = datasetNetworkManager.getPerformanceCounters();
            hbData.datasetNetPayloadBytesRead = datasetNetPC.getPayloadBytesRead();
            hbData.datasetNetPayloadBytesWritten = datasetNetPC.getPayloadBytesWritten();
            hbData.datasetNetSignalingBytesRead = datasetNetPC.getSignalingBytesRead();
            hbData.datasetNetSignalingBytesWritten = datasetNetPC.getSignalingBytesWritten();

            IPCPerformanceCounters ipcPC = ipc.getPerformanceCounters();
            hbData.ipcMessagesSent = ipcPC.getMessageSentCount();
            hbData.ipcMessageBytesSent = ipcPC.getMessageBytesSent();
            hbData.ipcMessagesReceived = ipcPC.getMessageReceivedCount();
            hbData.ipcMessageBytesReceived = ipcPC.getMessageBytesReceived();

            try {
                cc.nodeHeartbeat(id, hbData);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private class ProfileDumpTask extends TimerTask {
        private IClusterController cc;

        public ProfileDumpTask(IClusterController cc) {
            this.cc = cc;
        }

        @Override
        public void run() {
            try {
                FutureValue<List<JobProfile>> fv = new FutureValue<List<JobProfile>>();
                BuildJobProfilesWork bjpw = new BuildJobProfilesWork(NodeControllerService.this, fv);
                queue.scheduleAndSync(bjpw);
                List<JobProfile> profiles = fv.get();
                if (!profiles.isEmpty()) {
                    cc.reportProfile(id, profiles);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private final class NodeControllerIPCI implements IIPCI {
        @Override
        public void deliverIncomingMessage(IIPCHandle handle, long mid, long rmid, Object payload, Exception exception) {
            CCNCFunctions.Function fn = (CCNCFunctions.Function) payload;
            switch (fn.getFunctionId()) {
                case SEND_APPLICATION_MESSAGE: {
                    CCNCFunctions.SendApplicationMessageFunction amf = (CCNCFunctions.SendApplicationMessageFunction) fn;
                    queue.schedule(new ApplicationMessageWork(NodeControllerService.this, amf.getMessage(), amf
                            .getDeploymentId(), amf.getNodeId()));
                    return;
                }
                case START_TASKS: {
                    CCNCFunctions.StartTasksFunction stf = (CCNCFunctions.StartTasksFunction) fn;
                    queue.schedule(new StartTasksWork(NodeControllerService.this, stf.getDeploymentId(),
                            stf.getJobId(), stf.getPlanBytes(), stf.getTaskDescriptors(), stf.getConnectorPolicies(),
                            stf.getFlags()));
                    return;
                }

                case ABORT_TASKS: {
                    CCNCFunctions.AbortTasksFunction atf = (CCNCFunctions.AbortTasksFunction) fn;
                    queue.schedule(new AbortTasksWork(NodeControllerService.this, atf.getJobId(), atf.getTasks()));
                    return;
                }

                case CLEANUP_JOBLET: {
                    CCNCFunctions.CleanupJobletFunction cjf = (CCNCFunctions.CleanupJobletFunction) fn;
                    queue.schedule(new CleanupJobletWork(NodeControllerService.this, cjf.getJobId(), cjf.getStatus()));
                    return;
                }

                case REPORT_PARTITION_AVAILABILITY: {
                    CCNCFunctions.ReportPartitionAvailabilityFunction rpaf = (CCNCFunctions.ReportPartitionAvailabilityFunction) fn;
                    queue.schedule(new ReportPartitionAvailabilityWork(NodeControllerService.this, rpaf
                            .getPartitionId(), rpaf.getNetworkAddress()));
                    return;
                }

                case NODE_REGISTRATION_RESULT: {
                    CCNCFunctions.NodeRegistrationResult nrrf = (CCNCFunctions.NodeRegistrationResult) fn;
                    setNodeRegistrationResult(nrrf.getNodeParameters(), nrrf.getException());
                    return;
                }

                case GET_NODE_CONTROLLERS_INFO_RESPONSE: {
                    CCNCFunctions.GetNodeControllersInfoResponseFunction gncirf = (CCNCFunctions.GetNodeControllersInfoResponseFunction) fn;
                    setNodeControllersInfo(gncirf.getNodeControllerInfos());
                    return;
                }

                case DEPLOY_BINARY: {
                    CCNCFunctions.DeployBinaryFunction ndbf = (CCNCFunctions.DeployBinaryFunction) fn;
                    queue.schedule(new DeployBinaryWork(NodeControllerService.this, ndbf.getDeploymentId(), ndbf
                            .getBinaryURLs()));
                    return;
                }

                case UNDEPLOY_BINARY: {
                    CCNCFunctions.UnDeployBinaryFunction ndbf = (CCNCFunctions.UnDeployBinaryFunction) fn;
                    queue.schedule(new UnDeployBinaryWork(NodeControllerService.this, ndbf.getDeploymentId()));
                    return;
                }
            }
            throw new IllegalArgumentException("Unknown function: " + fn.getFunctionId());

        }
    }

    public void sendApplicationMessageToCC(byte[] data, DeploymentId deploymentId, String nodeId) throws Exception {
        ccs.sendApplicationMessageToCC(data, deploymentId, nodeId);
    }

    public IDatasetPartitionManager getDatasetPartitionManager() {
        return datasetPartitionManager;
    }
}