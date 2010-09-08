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

import edu.uci.ics.hyracks.api.comm.Endpoint;
import edu.uci.ics.hyracks.api.comm.IConnectionDemultiplexer;
import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksContext;
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
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobPlan;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.statistics.StageletStatistics;
import edu.uci.ics.hyracks.control.common.AbstractRemoteService;
import edu.uci.ics.hyracks.control.common.application.ApplicationContext;
import edu.uci.ics.hyracks.control.common.context.ServerContext;
import edu.uci.ics.hyracks.control.nc.comm.ConnectionManager;
import edu.uci.ics.hyracks.control.nc.comm.DemuxDataReceiveListenerFactory;
import edu.uci.ics.hyracks.control.nc.runtime.HyracksContext;
import edu.uci.ics.hyracks.control.nc.runtime.OperatorRunnable;

public class NodeControllerService extends AbstractRemoteService implements INodeController {
    private static final long serialVersionUID = 1L;

    private NCConfig ncConfig;

    private final String id;

    private final IHyracksContext ctx;

    private final NodeCapability nodeCapability;

    private final ConnectionManager connectionManager;

    private final Timer timer;

    private IClusterController ccs;

    private Map<UUID, Joblet> jobletMap;

    private Executor executor;

    private NodeParameters nodeParameters;

    private final ServerContext serverCtx;

    private final Map<String, ApplicationContext> applications;

    public NodeControllerService(NCConfig ncConfig) throws Exception {
        this.ncConfig = ncConfig;
        id = ncConfig.nodeId;
        this.ctx = new HyracksContext(ncConfig.frameSize);
        if (id == null) {
            throw new Exception("id not set");
        }
        nodeCapability = computeNodeCapability();
        connectionManager = new ConnectionManager(ctx, getIpAddress(ncConfig));
        jobletMap = new HashMap<UUID, Joblet>();
        executor = Executors.newCachedThreadPool();
        timer = new Timer(true);
        serverCtx = new ServerContext(ServerContext.ServerType.NODE_CONTROLLER, new File(new File(
                NodeControllerService.class.getName()), id));
        applications = new Hashtable<String, ApplicationContext>();
    }

    private static Logger LOGGER = Logger.getLogger(NodeControllerService.class.getName());

    @Override
    public void start() throws Exception {
        LOGGER.log(Level.INFO, "Starting NodeControllerService");
        connectionManager.start();
        Registry registry = LocateRegistry.getRegistry(ncConfig.ccHost, ncConfig.ccPort);
        IClusterController cc = (IClusterController) registry.lookup(IClusterController.class.getName());
        this.nodeParameters = cc.registerNode(this);

        // Schedule heartbeat generator.
        timer.schedule(new HeartbeatTask(cc), 0, nodeParameters.getHeartbeatPeriod());

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
    public Map<PortInstanceId, Endpoint> initializeJobletPhase1(String appName, UUID jobId, byte[] planBytes,
            UUID stageId, int attempt, Map<ActivityNodeId, Set<Integer>> tasks,
            Map<OperatorDescriptorId, Set<Integer>> opPartitions) throws Exception {
        try {
            LOGGER.log(Level.INFO, String.valueOf(jobId) + "[" + id + ":" + stageId + "]: Initializing Joblet Phase 1");

            ApplicationContext appCtx = applications.get(appName);
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

            final Joblet joblet = getLocalJoblet(jobId);

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
                    IOperatorNodePushable hon = han.createPushRuntime(ctx, joblet.getEnvironment(op, i), rdp, i,
                            opPartitions.get(op.getOperatorId()).size());
                    OperatorRunnable or = new OperatorRunnable(ctx, hon);
                    stagelet.setOperator(op.getOperatorId(), i, or);
                    if (inputs != null) {
                        for (int j = 0; j < inputs.size(); ++j) {
                            if (j >= 1) {
                                throw new IllegalStateException();
                            }
                            IConnectorDescriptor conn = inputs.get(j);
                            OperatorDescriptorId producerOpId = plan.getJobSpecification().getProducer(conn)
                                    .getOperatorId();
                            OperatorDescriptorId consumerOpId = plan.getJobSpecification().getConsumer(conn)
                                    .getOperatorId();
                            Endpoint endpoint = new Endpoint(connectionManager.getNetworkAddress(), i);
                            endpointList.add(endpoint);
                            DemuxDataReceiveListenerFactory drlf = new DemuxDataReceiveListenerFactory(ctx, jobId,
                                    stageId);
                            connectionManager.acceptConnection(endpoint.getEndpointId(), drlf);
                            PortInstanceId piId = new PortInstanceId(op.getOperatorId(), Direction.INPUT, plan
                                    .getTaskInputMap().get(hanId).get(j), i);
                            if (LOGGER.isLoggable(Level.FINEST)) {
                                LOGGER.finest("Created endpoint " + piId + " -> " + endpoint);
                            }
                            portMap.put(piId, endpoint);
                            IFrameReader reader = createReader(conn, drlf, i, plan, stagelet,
                                    opPartitions.get(producerOpId).size(), opPartitions.get(consumerOpId).size());
                            or.setFrameReader(reader);
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

    private IFrameReader createReader(final IConnectorDescriptor conn, IConnectionDemultiplexer demux,
            final int receiverIndex, JobPlan plan, final Stagelet stagelet, int nProducerCount, int nConsumerCount)
            throws HyracksDataException {
        final IFrameReader reader = conn.createReceiveSideReader(ctx, plan.getJobSpecification()
                .getConnectorRecordDescriptor(conn), demux, receiverIndex, nProducerCount, nConsumerCount);

        return plan.getJobFlags().contains(JobFlag.COLLECT_FRAME_COUNTS) ? new IFrameReader() {
            private int frameCount;

            @Override
            public void open() throws HyracksDataException {
                frameCount = 0;
                reader.open();
            }

            @Override
            public boolean nextFrame(ByteBuffer buffer) throws HyracksDataException {
                boolean status = reader.nextFrame(buffer);
                if (status) {
                    ++frameCount;
                }
                return status;
            }

            @Override
            public void close() throws HyracksDataException {
                reader.close();
                stagelet.getStatistics()
                        .getStatisticsMap()
                        .put("framecount." + conn.getConnectorId().getId() + ".receiver." + receiverIndex,
                                String.valueOf(frameCount));
            }
        } : reader;
    }

    @Override
    public void initializeJobletPhase2(String appName, UUID jobId, byte[] planBytes, UUID stageId,
            Map<ActivityNodeId, Set<Integer>> tasks, Map<OperatorDescriptorId, Set<Integer>> opPartitions,
            final Map<PortInstanceId, Endpoint> globalPortMap) throws Exception {
        try {
            LOGGER.log(Level.INFO, String.valueOf(jobId) + "[" + id + ":" + stageId + "]: Initializing Joblet Phase 2");
            ApplicationContext appCtx = applications.get(appName);
            final JobPlan plan = (JobPlan) appCtx.deserialize(planBytes);

            final Joblet ji = getLocalJoblet(jobId);
            Stagelet si = (Stagelet) ji.getStagelet(stageId);
            final Map<OperatorInstanceId, OperatorRunnable> honMap = si.getOperatorMap();

            final Stagelet stagelet = (Stagelet) ji.getStagelet(stageId);

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
                                    return createWriter(connectionManager.connect(ep.getNetworkAddress(),
                                            ep.getEndpointId(), senderIndex), plan, conn, senderIndex, index, stagelet);
                                }
                            };
                            or.setFrameWriter(j, conn.createSendSideWriter(ctx, plan.getJobSpecification()
                                    .getConnectorRecordDescriptor(conn), edwFactory, i, opPartitions.get(producerOpId)
                                    .size(), opPartitions.get(consumerOpId).size()), spec
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

    private IFrameWriter createWriter(final IFrameWriter writer, JobPlan plan, final IConnectorDescriptor conn,
            final int senderIndex, final int receiverIndex, final Stagelet stagelet) throws HyracksDataException {
        return plan.getJobFlags().contains(JobFlag.COLLECT_FRAME_COUNTS) ? new IFrameWriter() {
            private int frameCount;

            @Override
            public void open() throws HyracksDataException {
                frameCount = 0;
                writer.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                ++frameCount;
                writer.nextFrame(buffer);
            }

            @Override
            public void close() throws HyracksDataException {
                writer.close();
                stagelet.getStatistics()
                        .getStatisticsMap()
                        .put("framecount." + conn.getConnectorId().getId() + ".sender." + senderIndex + "."
                                + receiverIndex, String.valueOf(frameCount));
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

    private synchronized Joblet getLocalJoblet(UUID jobId) throws Exception {
        Joblet ji = jobletMap.get(jobId);
        if (ji == null) {
            ji = new Joblet(this, jobId);
            jobletMap.put(jobId, ji);
        }
        return ji;
    }

    public Executor getExecutor() {
        return executor;
    }

    @Override
    public synchronized void cleanUpJob(UUID jobId) throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Cleaning up after job: " + jobId);
        }
        jobletMap.remove(jobId);
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

    public void notifyStageComplete(UUID jobId, UUID stageId, int attempt, StageletStatistics stats) throws Exception {
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

    @Override
    public synchronized void abortJoblet(UUID jobId, UUID stageId) throws Exception {
        Joblet ji = jobletMap.get(jobId);
        if (ji != null) {
            Stagelet stagelet = ji.getStagelet(stageId);
            if (stagelet != null) {
                stagelet.abort();
                connectionManager.abortConnections(jobId, stageId);
            }
        }
    }

    @Override
    public void createApplication(String appName, boolean deployHar) throws Exception {
        ApplicationContext appCtx;
        synchronized (applications) {
            if (applications.containsKey(appName)) {
                throw new HyracksException("Duplicate application with name: " + appName + " being created.");
            }
            appCtx = new ApplicationContext(serverCtx, appName);
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