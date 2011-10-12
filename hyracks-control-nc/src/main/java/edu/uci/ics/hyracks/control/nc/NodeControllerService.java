/*
 * Copyright 2009-2010 by The Regents of the University of California
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
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.net.InetAddress;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.context.IHyracksRootContext;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.dataflow.connectors.IConnectorPolicy;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.control.common.AbstractRemoteService;
import edu.uci.ics.hyracks.control.common.base.IClusterController;
import edu.uci.ics.hyracks.control.common.base.INodeController;
import edu.uci.ics.hyracks.control.common.context.ServerContext;
import edu.uci.ics.hyracks.control.common.controllers.NCConfig;
import edu.uci.ics.hyracks.control.common.controllers.NodeParameters;
import edu.uci.ics.hyracks.control.common.controllers.NodeRegistration;
import edu.uci.ics.hyracks.control.common.heartbeat.HeartbeatData;
import edu.uci.ics.hyracks.control.common.job.TaskAttemptDescriptor;
import edu.uci.ics.hyracks.control.common.job.profiling.om.JobProfile;
import edu.uci.ics.hyracks.control.common.work.FutureValue;
import edu.uci.ics.hyracks.control.common.work.WorkQueue;
import edu.uci.ics.hyracks.control.nc.application.NCApplicationContext;
import edu.uci.ics.hyracks.control.nc.io.IOManager;
import edu.uci.ics.hyracks.control.nc.net.ConnectionManager;
import edu.uci.ics.hyracks.control.nc.partitions.PartitionManager;
import edu.uci.ics.hyracks.control.nc.runtime.RootHyracksContext;
import edu.uci.ics.hyracks.control.nc.work.AbortTasksWork;
import edu.uci.ics.hyracks.control.nc.work.BuildJobProfilesWork;
import edu.uci.ics.hyracks.control.nc.work.CleanupJobWork;
import edu.uci.ics.hyracks.control.nc.work.CreateApplicationWork;
import edu.uci.ics.hyracks.control.nc.work.DestroyApplicationWork;
import edu.uci.ics.hyracks.control.nc.work.ReportPartitionAvailabilityWork;
import edu.uci.ics.hyracks.control.nc.work.StartTasksWork;

public class NodeControllerService extends AbstractRemoteService implements INodeController {
    private static Logger LOGGER = Logger.getLogger(NodeControllerService.class.getName());

    private static final long serialVersionUID = 1L;

    private NCConfig ncConfig;

    private final String id;

    private final IHyracksRootContext ctx;

    private final PartitionManager partitionManager;

    private final ConnectionManager connectionManager;

    private final WorkQueue queue;

    private final Timer timer;

    private IClusterController ccs;

    private final Map<JobId, Joblet> jobletMap;

    private final Executor executor;

    private NodeParameters nodeParameters;

    private final ServerContext serverCtx;

    private final Map<String, NCApplicationContext> applications;

    private final MemoryMXBean memoryMXBean;

    private final ThreadMXBean threadMXBean;

    private final RuntimeMXBean runtimeMXBean;

    private final OperatingSystemMXBean osMXBean;

    public NodeControllerService(NCConfig ncConfig) throws Exception {
        this.ncConfig = ncConfig;
        id = ncConfig.nodeId;
        executor = Executors.newCachedThreadPool();
        this.ctx = new RootHyracksContext(ncConfig.frameSize, new IOManager(getDevices(ncConfig.ioDevices), executor));
        if (id == null) {
            throw new Exception("id not set");
        }
        connectionManager = new ConnectionManager(ctx, getIpAddress(ncConfig));
        partitionManager = new PartitionManager(this);
        connectionManager.setPartitionRequestListener(partitionManager);

        queue = new WorkQueue();
        jobletMap = new Hashtable<JobId, Joblet>();
        timer = new Timer(true);
        serverCtx = new ServerContext(ServerContext.ServerType.NODE_CONTROLLER, new File(new File(
                NodeControllerService.class.getName()), id));
        applications = new Hashtable<String, NCApplicationContext>();
        memoryMXBean = ManagementFactory.getMemoryMXBean();
        threadMXBean = ManagementFactory.getThreadMXBean();
        runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        osMXBean = ManagementFactory.getOperatingSystemMXBean();
    }

    public IHyracksRootContext getRootContext() {
        return ctx;
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

    @Override
    public void start() throws Exception {
        LOGGER.log(Level.INFO, "Starting NodeControllerService");
        connectionManager.start();
        Registry registry = LocateRegistry.getRegistry(ncConfig.ccHost, ncConfig.ccPort);
        IClusterController cc = (IClusterController) registry.lookup(IClusterController.class.getName());
        this.nodeParameters = cc.registerNode(new NodeRegistration(this, id, ncConfig, connectionManager
                .getNetworkAddress(), osMXBean.getName(), osMXBean.getArch(), osMXBean.getVersion(), osMXBean
                .getAvailableProcessors()));

        // Schedule heartbeat generator.
        timer.schedule(new HeartbeatTask(cc), 0, nodeParameters.getHeartbeatPeriod());

        if (nodeParameters.getProfileDumpPeriod() > 0) {
            // Schedule profile dump generator.
            timer.schedule(new ProfileDumpTask(cc), 0, nodeParameters.getProfileDumpPeriod());
        }

        LOGGER.log(Level.INFO, "Started NodeControllerService");
    }

    @Override
    public void stop() throws Exception {
        LOGGER.log(Level.INFO, "Stopping NodeControllerService");
        partitionManager.close();
        connectionManager.stop();
        LOGGER.log(Level.INFO, "Stopped NodeControllerService");
    }

    @Override
    public String getId() {
        return id;
    }

    public ServerContext getServerContext() {
        return serverCtx;
    }

    public Map<String, NCApplicationContext> getApplications() {
        return applications;
    }

    public Map<JobId, Joblet> getJobletMap() {
        return jobletMap;
    }

    public ConnectionManager getConnectionManager() {
        return connectionManager;
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

    @Override
    public void startTasks(String appName, final JobId jobId, byte[] jagBytes,
            List<TaskAttemptDescriptor> taskDescriptors,
            Map<ConnectorDescriptorId, IConnectorPolicy> connectorPoliciesMap, byte[] ctxVarBytes) throws Exception {
        StartTasksWork stw = new StartTasksWork(this, appName, jobId, jagBytes, taskDescriptors, connectorPoliciesMap);
        queue.scheduleAndSync(stw);
    }

    @Override
    public void cleanUpJob(JobId jobId) throws Exception {
        CleanupJobWork cjw = new CleanupJobWork(this, jobId);
        queue.scheduleAndSync(cjw);
    }

    @Override
    public void notifyRegistration(IClusterController ccs) throws Exception {
        this.ccs = ccs;
    }

    @Override
    public NCConfig getConfiguration() throws Exception {
        return ncConfig;
    }

    @Override
    public synchronized void abortTasks(JobId jobId, List<TaskAttemptId> tasks) throws Exception {
        AbortTasksWork atw = new AbortTasksWork(this, jobId, tasks);
        queue.scheduleAndSync(atw);
    }

    @Override
    public void createApplication(String appName, boolean deployHar, byte[] serializedDistributedState)
            throws Exception {
        CreateApplicationWork caw = new CreateApplicationWork(this, appName, deployHar, serializedDistributedState);
        queue.scheduleAndSync(caw);
    }

    @Override
    public void destroyApplication(String appName) throws Exception {
        DestroyApplicationWork daw = new DestroyApplicationWork(this, appName);
        queue.scheduleAndSync(daw);
    }

    @Override
    public void reportPartitionAvailability(PartitionId pid, NetworkAddress networkAddress) throws Exception {
        ReportPartitionAvailabilityWork rpaw = new ReportPartitionAvailabilityWork(this, pid, networkAddress);
        queue.scheduleAndSync(rpaw);
    }

    private static InetAddress getIpAddress(NCConfig ncConfig) throws Exception {
        String ipaddrStr = ncConfig.dataIPAddress;
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
                FutureValue fv = new FutureValue();
                BuildJobProfilesWork bjpw = new BuildJobProfilesWork(NodeControllerService.this, fv);
                queue.scheduleAndSync(bjpw);
                List<JobProfile> profiles = (List<JobProfile>) fv.get();
                if (!profiles.isEmpty()) {
                    cc.reportProfile(id, profiles);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}