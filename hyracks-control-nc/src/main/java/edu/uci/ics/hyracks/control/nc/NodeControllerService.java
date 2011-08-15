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
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
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

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import edu.uci.ics.hyracks.api.application.INCApplicationContext;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.comm.IPartitionCollector;
import edu.uci.ics.hyracks.api.comm.IPartitionWriterFactory;
import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.comm.PartitionChannel;
import edu.uci.ics.hyracks.api.context.IHyracksRootContext;
import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.IActivity;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.dataflow.TaskId;
import edu.uci.ics.hyracks.api.dataflow.connectors.IConnectorPolicy;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobActivityGraph;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.naming.MultipartName;
import edu.uci.ics.hyracks.api.partitions.PartitionId;
import edu.uci.ics.hyracks.control.common.AbstractRemoteService;
import edu.uci.ics.hyracks.control.common.application.ApplicationContext;
import edu.uci.ics.hyracks.control.common.base.IClusterController;
import edu.uci.ics.hyracks.control.common.base.INodeController;
import edu.uci.ics.hyracks.control.common.context.ServerContext;
import edu.uci.ics.hyracks.control.common.controllers.NCConfig;
import edu.uci.ics.hyracks.control.common.controllers.NodeCapability;
import edu.uci.ics.hyracks.control.common.controllers.NodeParameters;
import edu.uci.ics.hyracks.control.common.controllers.NodeRegistration;
import edu.uci.ics.hyracks.control.common.job.TaskAttemptDescriptor;
import edu.uci.ics.hyracks.control.common.job.profiling.om.JobProfile;
import edu.uci.ics.hyracks.control.common.job.profiling.om.JobletProfile;
import edu.uci.ics.hyracks.control.common.job.profiling.om.TaskProfile;
import edu.uci.ics.hyracks.control.nc.application.NCApplicationContext;
import edu.uci.ics.hyracks.control.nc.io.IOManager;
import edu.uci.ics.hyracks.control.nc.net.ConnectionManager;
import edu.uci.ics.hyracks.control.nc.net.NetworkInputChannel;
import edu.uci.ics.hyracks.control.nc.partitions.MaterializedPartitionWriter;
import edu.uci.ics.hyracks.control.nc.partitions.PartitionManager;
import edu.uci.ics.hyracks.control.nc.partitions.PipelinedPartition;
import edu.uci.ics.hyracks.control.nc.partitions.ReceiveSideMaterializingCollector;
import edu.uci.ics.hyracks.control.nc.runtime.RootHyracksContext;

public class NodeControllerService extends AbstractRemoteService implements INodeController {
    private static Logger LOGGER = Logger.getLogger(NodeControllerService.class.getName());

    private static final long serialVersionUID = 1L;

    private NCConfig ncConfig;

    private final String id;

    private final IHyracksRootContext ctx;

    private final NodeCapability nodeCapability;

    private final PartitionManager partitionManager;

    private final ConnectionManager connectionManager;

    private final Timer timer;

    private IClusterController ccs;

    private final Map<JobId, Joblet> jobletMap;

    private final Executor executor;

    private NodeParameters nodeParameters;

    private final ServerContext serverCtx;

    private final Map<String, NCApplicationContext> applications;

    public NodeControllerService(NCConfig ncConfig) throws Exception {
        this.ncConfig = ncConfig;
        id = ncConfig.nodeId;
        executor = Executors.newCachedThreadPool();
        this.ctx = new RootHyracksContext(ncConfig.frameSize, new IOManager(getDevices(ncConfig.ioDevices), executor));
        if (id == null) {
            throw new Exception("id not set");
        }
        nodeCapability = computeNodeCapability();
        connectionManager = new ConnectionManager(ctx, getIpAddress(ncConfig));
        partitionManager = new PartitionManager(this);
        connectionManager.setPartitionRequestListener(partitionManager);

        jobletMap = new Hashtable<JobId, Joblet>();
        timer = new Timer(true);
        serverCtx = new ServerContext(ServerContext.ServerType.NODE_CONTROLLER, new File(new File(
                NodeControllerService.class.getName()), id));
        applications = new Hashtable<String, NCApplicationContext>();
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
                .getNetworkAddress()));

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

    @Override
    public NodeCapability getNodeCapability() throws Exception {
        return nodeCapability;
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

    private static NodeCapability computeNodeCapability() {
        NodeCapability nc = new NodeCapability();
        nc.setCPUCount(Runtime.getRuntime().availableProcessors());
        return nc;
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

    @Override
    public void startTasks(String appName, final JobId jobId, byte[] jagBytes,
            List<TaskAttemptDescriptor> taskDescriptors,
            Map<ConnectorDescriptorId, IConnectorPolicy> connectorPoliciesMap, byte[] ctxVarBytes) throws Exception {
        try {
            NCApplicationContext appCtx = applications.get(appName);
            final JobActivityGraph plan = (JobActivityGraph) appCtx.deserialize(jagBytes);
            Map<MultipartName, Object> ctxVarMap = (Map<MultipartName, Object>) appCtx.deserialize(ctxVarBytes);

            IRecordDescriptorProvider rdp = new IRecordDescriptorProvider() {
                @Override
                public RecordDescriptor getOutputRecordDescriptor(OperatorDescriptorId opId, int outputIndex) {
                    return plan.getJobSpecification().getOperatorOutputRecordDescriptor(opId, outputIndex);
                }

                @Override
                public RecordDescriptor getInputRecordDescriptor(OperatorDescriptorId opId, int inputIndex) {
                    return plan.getJobSpecification().getOperatorInputRecordDescriptor(opId, inputIndex);
                }
            };

            final Joblet joblet = getOrCreateLocalJoblet(jobId, appCtx);

            for (TaskAttemptDescriptor td : taskDescriptors) {
                TaskAttemptId taId = td.getTaskAttemptId();
                TaskId tid = taId.getTaskId();
                IActivity han = plan.getActivityNodeMap().get(tid.getActivityId());
                if (LOGGER.isLoggable(Level.INFO)) {
                    LOGGER.info("Initializing " + taId + " -> " + han);
                }
                final int partition = tid.getPartition();
                Map<MultipartName, Object> inputGlobalVariables = createInputGlobalVariables(ctxVarMap, han);
                Task task = new Task(joblet, taId, han.getClass().getName(), executor);
                IOperatorEnvironment env = joblet.getEnvironment(tid.getActivityId().getOperatorDescriptorId(),
                        tid.getPartition());
                IOperatorNodePushable operator = han.createPushRuntime(task, env, rdp, partition,
                        td.getPartitionCount());

                List<IPartitionCollector> collectors = new ArrayList<IPartitionCollector>();

                List<IConnectorDescriptor> inputs = plan.getActivityInputConnectorDescriptors(tid.getActivityId());
                if (inputs != null) {
                    for (int i = 0; i < inputs.size(); ++i) {
                        IConnectorDescriptor conn = inputs.get(i);
                        IConnectorPolicy cPolicy = connectorPoliciesMap.get(conn.getConnectorId());
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info("input: " + i + ": " + conn.getConnectorId());
                        }
                        RecordDescriptor recordDesc = plan.getJobSpecification().getConnectorRecordDescriptor(conn);
                        IPartitionCollector collector = createPartitionCollector(td, partition, task, i, conn,
                                recordDesc, cPolicy);
                        collectors.add(collector);
                    }
                }
                List<IConnectorDescriptor> outputs = plan.getActivityOutputConnectorDescriptors(tid.getActivityId());
                if (outputs != null) {
                    for (int i = 0; i < outputs.size(); ++i) {
                        final IConnectorDescriptor conn = outputs.get(i);
                        RecordDescriptor recordDesc = plan.getJobSpecification().getConnectorRecordDescriptor(conn);
                        IConnectorPolicy cPolicy = connectorPoliciesMap.get(conn.getConnectorId());

                        IPartitionWriterFactory pwFactory = createPartitionWriterFactory(cPolicy, jobId, conn,
                                partition, taId);

                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info("output: " + i + ": " + conn.getConnectorId());
                        }
                        IFrameWriter writer = conn.createPartitioner(task, recordDesc, pwFactory, partition,
                                td.getPartitionCount(), td.getOutputPartitionCounts()[i]);
                        operator.setOutputFrameWriter(i, writer, recordDesc);
                    }
                }

                task.setTaskRuntime(collectors.toArray(new IPartitionCollector[collectors.size()]), operator);
                joblet.addTask(task);

                task.start();
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    private Map<MultipartName, Object> createInputGlobalVariables(Map<MultipartName, Object> ctxVarMap, IActivity han) {
        Map<MultipartName, Object> gVars = new HashMap<MultipartName, Object>();
        //        for (MultipartName inVar : han.getInputVariables()) {
        //            gVars.put(inVar, ctxVarMap.get(inVar));
        //        }
        return gVars;
    }

    private IPartitionCollector createPartitionCollector(TaskAttemptDescriptor td, final int partition, Task task,
            int i, IConnectorDescriptor conn, RecordDescriptor recordDesc, IConnectorPolicy cPolicy)
            throws HyracksDataException {
        IPartitionCollector collector = conn.createPartitionCollector(task, recordDesc, partition,
                td.getInputPartitionCounts()[i], td.getPartitionCount());
        if (cPolicy.materializeOnReceiveSide()) {
            return new ReceiveSideMaterializingCollector(ctx, partitionManager, collector, task.getTaskAttemptId(),
                    executor);
        } else {
            return collector;
        }
    }

    private IPartitionWriterFactory createPartitionWriterFactory(IConnectorPolicy cPolicy, final JobId jobId,
            final IConnectorDescriptor conn, final int senderIndex, final TaskAttemptId taId) {
        if (cPolicy.materializeOnSendSide()) {
            return new IPartitionWriterFactory() {
                @Override
                public IFrameWriter createFrameWriter(int receiverIndex) throws HyracksDataException {
                    return new MaterializedPartitionWriter(ctx, partitionManager, new PartitionId(jobId,
                            conn.getConnectorId(), senderIndex, receiverIndex), taId, executor);
                }
            };
        } else {
            return new IPartitionWriterFactory() {
                @Override
                public IFrameWriter createFrameWriter(int receiverIndex) throws HyracksDataException {
                    return new PipelinedPartition(partitionManager, new PartitionId(jobId, conn.getConnectorId(),
                            senderIndex, receiverIndex), taId);
                }
            };
        }
    }

    private synchronized Joblet getOrCreateLocalJoblet(JobId jobId, INCApplicationContext appCtx) throws Exception {
        Joblet ji = jobletMap.get(jobId);
        if (ji == null) {
            ji = new Joblet(this, jobId, appCtx);
            jobletMap.put(jobId, ji);
        }
        return ji;
    }

    public Executor getExecutor() {
        return executor;
    }

    @Override
    public void cleanUpJob(JobId jobId) throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Cleaning up after job: " + jobId);
        }
        Joblet joblet = jobletMap.remove(jobId);
        if (joblet != null) {
            partitionManager.unregisterPartitions(jobId);
            joblet.close();
        }
    }

    public void notifyTaskComplete(JobId jobId, TaskAttemptId taskId, TaskProfile taskProfile) throws Exception {
        try {
            ccs.notifyTaskComplete(jobId, taskId, id, taskProfile);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    public void notifyTaskFailed(JobId jobId, TaskAttemptId taskId, Exception exception) {
        try {
            ccs.notifyTaskFailure(jobId, taskId, id, exception);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void notifyRegistration(IClusterController ccs) throws Exception {
        this.ccs = ccs;
    }

    @Override
    public NCConfig getConfiguration() throws Exception {
        return ncConfig;
    }

    private class HeartbeatTask extends TimerTask {
        private IClusterController cc;

        public HeartbeatTask(IClusterController cc) {
            this.cc = cc;
        }

        @Override
        public void run() {
            try {
                cc.nodeHeartbeat(id);
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
                List<JobProfile> profiles;
                synchronized (NodeControllerService.this) {
                    profiles = new ArrayList<JobProfile>();
                    for (Joblet ji : jobletMap.values()) {
                        profiles.add(new JobProfile(ji.getJobId()));
                    }
                }
                for (JobProfile jProfile : profiles) {
                    Joblet ji;
                    JobletProfile jobletProfile = new JobletProfile(id);
                    synchronized (NodeControllerService.this) {
                        ji = jobletMap.get(jProfile.getJobId());
                    }
                    if (ji != null) {
                        ji.dumpProfile(jobletProfile);
                        jProfile.getJobletProfiles().put(id, jobletProfile);
                    }
                }
                if (!profiles.isEmpty()) {
                    cc.reportProfile(id, profiles);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public synchronized void abortTasks(JobId jobId, List<TaskAttemptId> tasks) throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Aborting Tasks: " + jobId + ":" + tasks);
        }
        Joblet ji = jobletMap.get(jobId);
        if (ji != null) {
            Map<TaskAttemptId, Task> taskMap = ji.getTaskMap();
            for (TaskAttemptId taId : tasks) {
                Task task = taskMap.get(taId);
                if (task != null) {
                    task.abort();
                }
            }
            ji.close();
        }
    }

    @Override
    public void createApplication(String appName, boolean deployHar, byte[] serializedDistributedState)
            throws Exception {
        NCApplicationContext appCtx;
        synchronized (applications) {
            if (applications.containsKey(appName)) {
                throw new HyracksException("Duplicate application with name: " + appName + " being created.");
            }
            appCtx = new NCApplicationContext(serverCtx, ctx, appName, id);
            applications.put(appName, appCtx);
        }
        if (deployHar) {
            HttpClient hc = new DefaultHttpClient();
            HttpGet get = new HttpGet("http://" + ncConfig.ccHost + ":"
                    + nodeParameters.getClusterControllerInfo().getWebPort() + "/applications/" + appName);
            HttpResponse response = hc.execute(get);
            InputStream is = response.getEntity().getContent();
            OutputStream os = appCtx.getHarOutputStream();
            try {
                IOUtils.copyLarge(is, os);
            } finally {
                os.close();
                is.close();
            }
        }
        appCtx.initializeClassPath();
        appCtx.setDistributedState((Serializable) appCtx.deserialize(serializedDistributedState));
        appCtx.initialize();
    }

    @Override
    public void destroyApplication(String appName) throws Exception {
        ApplicationContext appCtx = applications.remove(appName);
        if (appCtx != null) {
            appCtx.deinitialize();
        }
    }

    @Override
    public void reportPartitionAvailability(PartitionId pid, NetworkAddress networkAddress) throws Exception {
        Joblet ji = jobletMap.get(pid.getJobId());
        if (ji != null) {
            PartitionChannel channel = new PartitionChannel(pid, new NetworkInputChannel(ctx, connectionManager,
                    new InetSocketAddress(networkAddress.getIpAddress(), networkAddress.getPort()), pid, 1));
            ji.reportPartitionAvailability(channel);
        }
    }
}