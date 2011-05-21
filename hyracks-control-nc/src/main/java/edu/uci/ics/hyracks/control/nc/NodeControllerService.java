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
import java.nio.ByteBuffer;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
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
import edu.uci.ics.hyracks.api.comm.Endpoint;
import edu.uci.ics.hyracks.api.comm.IConnectionDemultiplexer;
import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksRootContext;
import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.control.IClusterController;
import edu.uci.ics.hyracks.api.control.INodeController;
import edu.uci.ics.hyracks.api.control.NCConfig;
import edu.uci.ics.hyracks.api.control.NodeCapability;
import edu.uci.ics.hyracks.api.control.NodeParameters;
import edu.uci.ics.hyracks.api.dataflow.ActivityNodeId;
import edu.uci.ics.hyracks.api.dataflow.Direction;
import edu.uci.ics.hyracks.api.dataflow.IActivityNode;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IEndpointDataWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.OperatorInstanceId;
import edu.uci.ics.hyracks.api.dataflow.PortInstanceId;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobPlan;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.profiling.counters.ICounter;
import edu.uci.ics.hyracks.api.job.profiling.om.JobProfile;
import edu.uci.ics.hyracks.api.job.profiling.om.JobletProfile;
import edu.uci.ics.hyracks.api.job.profiling.om.StageletProfile;
import edu.uci.ics.hyracks.control.common.AbstractRemoteService;
import edu.uci.ics.hyracks.control.common.application.ApplicationContext;
import edu.uci.ics.hyracks.control.common.context.ServerContext;
import edu.uci.ics.hyracks.control.nc.application.NCApplicationContext;
import edu.uci.ics.hyracks.control.nc.comm.ConnectionManager;
import edu.uci.ics.hyracks.control.nc.comm.DemuxDataReceiveListenerFactory;
import edu.uci.ics.hyracks.control.nc.io.IOManager;
import edu.uci.ics.hyracks.control.nc.runtime.OperatorRunnable;
import edu.uci.ics.hyracks.control.nc.runtime.RootHyracksContext;

public class NodeControllerService extends AbstractRemoteService implements INodeController {
    private static Logger LOGGER = Logger.getLogger(NodeControllerService.class.getName());

    private static final long serialVersionUID = 1L;

    private NCConfig ncConfig;

    private final String id;

    private final IHyracksRootContext ctx;

    private final NodeCapability nodeCapability;

    private final ConnectionManager connectionManager;

    private final Timer timer;

    private IClusterController ccs;

    private final Map<UUID, Joblet> jobletMap;

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
        jobletMap = new Hashtable<UUID, Joblet>();
        timer = new Timer(true);
        serverCtx = new ServerContext(ServerContext.ServerType.NODE_CONTROLLER, new File(new File(
                NodeControllerService.class.getName()), id));
        applications = new Hashtable<String, NCApplicationContext>();
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
        this.nodeParameters = cc.registerNode(this);

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
    public Map<PortInstanceId, Endpoint> initializeJobletPhase1(String appName, UUID jobId, int attempt,
            byte[] planBytes, UUID stageId, Map<ActivityNodeId, Set<Integer>> tasks,
            Map<OperatorDescriptorId, Integer> opNumPartitions) throws Exception {
        try {
            LOGGER.log(Level.INFO, String.valueOf(jobId) + "[" + id + ":" + stageId + "]: Initializing Joblet Phase 1");

            NCApplicationContext appCtx = applications.get(appName);
            final JobPlan plan = (JobPlan) appCtx.deserialize(planBytes);

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

            final Joblet joblet = getOrCreateLocalJoblet(jobId, attempt, appCtx);

            Stagelet stagelet = new Stagelet(joblet, stageId, attempt, id);
            joblet.setStagelet(stageId, stagelet);

            final Map<PortInstanceId, Endpoint> portMap = new HashMap<PortInstanceId, Endpoint>();
            Map<OperatorInstanceId, OperatorRunnable> honMap = stagelet.getOperatorMap();

            List<Endpoint> endpointList = new ArrayList<Endpoint>();

            for (ActivityNodeId hanId : tasks.keySet()) {
                IActivityNode han = plan.getActivityNodeMap().get(hanId);
                if (LOGGER.isLoggable(Level.FINEST)) {
                    LOGGER.finest("Initializing " + hanId + " -> " + han);
                }
                IOperatorDescriptor op = han.getOwner();
                List<IConnectorDescriptor> inputs = plan.getTaskInputs(hanId);
                for (int i : tasks.get(hanId)) {
                    IOperatorNodePushable hon = han.createPushRuntime(stagelet, joblet.getEnvironment(op, i), rdp, i,
                            opNumPartitions.get(op.getOperatorId()));
                    OperatorRunnable or = new OperatorRunnable(stagelet, hon, inputs == null ? 0 : inputs.size(),
                            executor);
                    stagelet.setOperator(op.getOperatorId(), i, or);
                    if (inputs != null) {
                        for (int j = 0; j < inputs.size(); ++j) {
                            IConnectorDescriptor conn = inputs.get(j);
                            OperatorDescriptorId producerOpId = plan.getJobSpecification().getProducer(conn)
                                    .getOperatorId();
                            OperatorDescriptorId consumerOpId = plan.getJobSpecification().getConsumer(conn)
                                    .getOperatorId();
                            Endpoint endpoint = new Endpoint(connectionManager.getNetworkAddress(), i);
                            endpointList.add(endpoint);
                            DemuxDataReceiveListenerFactory drlf = new DemuxDataReceiveListenerFactory(stagelet, jobId,
                                    stageId);
                            connectionManager.acceptConnection(endpoint.getEndpointId(), drlf);
                            PortInstanceId piId = new PortInstanceId(op.getOperatorId(), Direction.INPUT, plan
                                    .getTaskInputMap().get(hanId).get(j), i);
                            if (LOGGER.isLoggable(Level.FINEST)) {
                                LOGGER.finest("Created endpoint " + piId + " -> " + endpoint);
                            }
                            portMap.put(piId, endpoint);
                            IFrameReader reader = createReader(stagelet, conn, drlf, i, plan, stagelet,
                                    opNumPartitions.get(producerOpId), opNumPartitions.get(consumerOpId));
                            or.setFrameReader(j, reader);
                        }
                    }
                    honMap.put(new OperatorInstanceId(op.getOperatorId(), i), or);
                }
            }

            stagelet.setEndpointList(endpointList);

            return portMap;
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    private IFrameReader createReader(final IHyracksStageletContext stageletContext, final IConnectorDescriptor conn,
            IConnectionDemultiplexer demux, final int receiverIndex, JobPlan plan, final Stagelet stagelet,
            int nProducerCount, int nConsumerCount) throws HyracksDataException {
        final IFrameReader reader = conn.createReceiveSideReader(stageletContext, plan.getJobSpecification()
                .getConnectorRecordDescriptor(conn), demux, receiverIndex, nProducerCount, nConsumerCount);

        return plan.getJobFlags().contains(JobFlag.PROFILE_RUNTIME) ? new IFrameReader() {
            private ICounter openCounter = stageletContext.getCounterContext().getCounter(
                    conn.getConnectorId().getId() + ".receiver." + receiverIndex + ".open", true);
            private ICounter closeCounter = stageletContext.getCounterContext().getCounter(
                    conn.getConnectorId().getId() + ".receiver." + receiverIndex + ".close", true);
            private ICounter frameCounter = stageletContext.getCounterContext().getCounter(
                    conn.getConnectorId().getId() + ".receiver." + receiverIndex + ".nextFrame", true);

            @Override
            public void open() throws HyracksDataException {
                reader.open();
                openCounter.update(1);
            }

            @Override
            public boolean nextFrame(ByteBuffer buffer) throws HyracksDataException {
                boolean status = reader.nextFrame(buffer);
                if (status) {
                    frameCounter.update(1);
                }
                return status;
            }

            @Override
            public void close() throws HyracksDataException {
                reader.close();
                closeCounter.update(1);
            }
        } : reader;
    }

    @Override
    public void initializeJobletPhase2(String appName, UUID jobId, byte[] planBytes, UUID stageId,
            Map<ActivityNodeId, Set<Integer>> tasks, Map<OperatorDescriptorId, Integer> opNumPartitions,
            final Map<PortInstanceId, Endpoint> globalPortMap) throws Exception {
        try {
            LOGGER.log(Level.INFO, String.valueOf(jobId) + "[" + id + ":" + stageId + "]: Initializing Joblet Phase 2");
            ApplicationContext appCtx = applications.get(appName);
            final JobPlan plan = (JobPlan) appCtx.deserialize(planBytes);

            final Joblet ji = getLocalJoblet(jobId);
            final Stagelet stagelet = (Stagelet) ji.getStagelet(stageId);
            final Map<OperatorInstanceId, OperatorRunnable> honMap = stagelet.getOperatorMap();

            final JobSpecification spec = plan.getJobSpecification();

            for (ActivityNodeId hanId : tasks.keySet()) {
                IActivityNode han = plan.getActivityNodeMap().get(hanId);
                IOperatorDescriptor op = han.getOwner();
                List<IConnectorDescriptor> outputs = plan.getTaskOutputs(hanId);
                for (int i : tasks.get(hanId)) {
                    OperatorRunnable or = honMap.get(new OperatorInstanceId(op.getOperatorId(), i));
                    if (outputs != null) {
                        for (int j = 0; j < outputs.size(); ++j) {
                            final IConnectorDescriptor conn = outputs.get(j);
                            OperatorDescriptorId producerOpId = plan.getJobSpecification().getProducer(conn)
                                    .getOperatorId();
                            OperatorDescriptorId consumerOpId = plan.getJobSpecification().getConsumer(conn)
                                    .getOperatorId();
                            final int senderIndex = i;
                            IEndpointDataWriterFactory edwFactory = new IEndpointDataWriterFactory() {
                                @Override
                                public IFrameWriter createFrameWriter(int index) throws HyracksDataException {
                                    PortInstanceId piId = new PortInstanceId(spec.getConsumer(conn).getOperatorId(),
                                            Direction.INPUT, spec.getConsumerInputIndex(conn), index);
                                    Endpoint ep = globalPortMap.get(piId);
                                    if (ep == null) {
                                        LOGGER.info("Got null Endpoint for " + piId);
                                        throw new NullPointerException();
                                    }
                                    if (LOGGER.isLoggable(Level.FINEST)) {
                                        LOGGER.finest("Probed endpoint " + piId + " -> " + ep);
                                    }
                                    return createWriter(stagelet, connectionManager.connect(ep.getNetworkAddress(),
                                            ep.getEndpointId(), senderIndex), plan, conn, senderIndex, index, stagelet);
                                }
                            };
                            or.setFrameWriter(j, conn.createSendSideWriter(stagelet, plan.getJobSpecification()
                                    .getConnectorRecordDescriptor(conn), edwFactory, i, opNumPartitions
                                    .get(producerOpId), opNumPartitions.get(consumerOpId)), spec
                                    .getConnectorRecordDescriptor(conn));
                        }
                    }
                    stagelet.installRunnable(new OperatorInstanceId(op.getOperatorId(), i));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    private IFrameWriter createWriter(final IHyracksStageletContext stageletContext, final IFrameWriter writer,
            JobPlan plan, final IConnectorDescriptor conn, final int senderIndex, final int receiverIndex,
            final Stagelet stagelet) throws HyracksDataException {
        return plan.getJobFlags().contains(JobFlag.PROFILE_RUNTIME) ? new IFrameWriter() {
            private ICounter openCounter = stageletContext.getCounterContext().getCounter(
                    conn.getConnectorId().getId() + ".sender." + senderIndex + "." + receiverIndex + ".open", true);
            private ICounter closeCounter = stageletContext.getCounterContext().getCounter(
                    conn.getConnectorId().getId() + ".sender." + senderIndex + "." + receiverIndex + ".close", true);
            private ICounter frameCounter = stageletContext.getCounterContext()
                    .getCounter(
                            conn.getConnectorId().getId() + ".sender." + senderIndex + "." + receiverIndex
                                    + ".nextFrame", true);

            @Override
            public void open() throws HyracksDataException {
                writer.open();
                openCounter.update(1);
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                frameCounter.update(1);
                writer.nextFrame(buffer);
            }

            @Override
            public void close() throws HyracksDataException {
                closeCounter.update(1);
                writer.close();
            }

            @Override
            public void flush() throws HyracksDataException {
                writer.flush();
            }
        } : writer;
    }

    @Override
    public void commitJobletInitialization(UUID jobId, UUID stageId) throws Exception {
        final Joblet ji = getLocalJoblet(jobId);
        Stagelet si = (Stagelet) ji.getStagelet(stageId);
        for (Endpoint e : si.getEndpointList()) {
            connectionManager.unacceptConnection(e.getEndpointId());
        }
        si.setEndpointList(null);
    }

    private Joblet getLocalJoblet(UUID jobId) throws Exception {
        Joblet ji = jobletMap.get(jobId);
        return ji;
    }

    private Joblet getOrCreateLocalJoblet(UUID jobId, int attempt, INCApplicationContext appCtx) throws Exception {
        synchronized (jobletMap) {
            Joblet ji = jobletMap.get(jobId);
            if (ji == null || ji.getAttempt() != attempt) {
                ji = new Joblet(this, jobId, attempt, appCtx);
                jobletMap.put(jobId, ji);
            }
            return ji;
        }
    }

    public Executor getExecutor() {
        return executor;
    }

    @Override
    public void cleanUpJob(UUID jobId) throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Cleaning up after job: " + jobId);
        }
        Joblet joblet = jobletMap.remove(jobId);
        if (joblet != null) {
            joblet.close();
        }
        connectionManager.dumpStats();
    }

    @Override
    public void startStage(UUID jobId, UUID stageId) throws Exception {
        Joblet ji = jobletMap.get(jobId);
        if (ji != null) {
            Stagelet s = ji.getStagelet(stageId);
            if (s != null) {
                s.start();
            }
        }
    }

    public void notifyStageComplete(UUID jobId, UUID stageId, int attempt, StageletProfile stats) throws Exception {
        try {
            ccs.notifyStageletComplete(jobId, stageId, attempt, id, stats);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    public void notifyStageFailed(UUID jobId, UUID stageId, int attempt) throws Exception {
        try {
            ccs.notifyStageletFailure(jobId, stageId, attempt, id);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
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
                        profiles.add(new JobProfile(ji.getJobId(), ji.getAttempt()));
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
    public void abortJoblet(UUID jobId, int attempt) throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Aborting Job: " + jobId + ":" + attempt);
        }
        Joblet ji = jobletMap.get(jobId);
        if (ji != null) {
            if (ji.getAttempt() == attempt) {
                jobletMap.remove(jobId);
            }
            for (Stagelet stagelet : ji.getStageletMap().values()) {
                stagelet.abort();
                stagelet.close();
                connectionManager.abortConnections(jobId, stagelet.getStageId());
            }
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
}