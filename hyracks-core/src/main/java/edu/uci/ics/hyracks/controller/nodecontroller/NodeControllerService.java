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
package edu.uci.ics.hyracks.controller.nodecontroller;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
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

import edu.uci.ics.hyracks.api.comm.Endpoint;
import edu.uci.ics.hyracks.api.comm.IConnectionDemultiplexer;
import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.controller.IClusterController;
import edu.uci.ics.hyracks.api.controller.INodeController;
import edu.uci.ics.hyracks.api.controller.NodeCapability;
import edu.uci.ics.hyracks.api.controller.NodeParameters;
import edu.uci.ics.hyracks.api.dataflow.ActivityNodeId;
import edu.uci.ics.hyracks.api.dataflow.Direction;
import edu.uci.ics.hyracks.api.dataflow.IActivityNode;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IEndpointDataWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.OperatorInstanceId;
import edu.uci.ics.hyracks.api.dataflow.PortInstanceId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobPlan;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.statistics.StageletStatistics;
import edu.uci.ics.hyracks.comm.ConnectionManager;
import edu.uci.ics.hyracks.comm.DemuxDataReceiveListenerFactory;
import edu.uci.ics.hyracks.config.NCConfig;
import edu.uci.ics.hyracks.context.HyracksContext;
import edu.uci.ics.hyracks.controller.AbstractRemoteService;
import edu.uci.ics.hyracks.runtime.OperatorRunnable;

public class NodeControllerService extends AbstractRemoteService implements INodeController {
    private static final long serialVersionUID = 1L;

    private NCConfig ncConfig;

    private final String id;

    private final HyracksContext ctx;

    private final NodeCapability nodeCapability;

    private final ConnectionManager connectionManager;

    private final Timer timer;

    private IClusterController ccs;

    private Map<UUID, Joblet> jobletMap;

    private Executor executor;

    private NodeParameters nodeParameters;

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
        timer.schedule(new HeartbeatTask(cc), 0, nodeParameters.getHeartbeatPeriod() * 1000);

        LOGGER.log(Level.INFO, "Started NodeControllerService");
    }

    @Override
    public void stop() throws Exception {
        LOGGER.log(Level.INFO, "Stopping NodeControllerService");
        connectionManager.stop();
        LOGGER.log(Level.INFO, "Stopped NodeControllerService");
    }

    @Override
    public String getId() throws Exception {
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
    public Map<PortInstanceId, Endpoint> initializeJobletPhase1(UUID jobId, JobPlan plan, UUID stageId, int attempt,
        Set<ActivityNodeId> activities) throws Exception {
        LOGGER.log(Level.INFO, String.valueOf(jobId) + "[" + id + ":" + stageId + "]: Initializing Joblet Phase 1");

        final Joblet joblet = getLocalJoblet(jobId);

        Stagelet stagelet = new Stagelet(joblet, stageId, attempt, id);
        joblet.setStagelet(stageId, stagelet);

        final Map<PortInstanceId, Endpoint> portMap = new HashMap<PortInstanceId, Endpoint>();
        Map<OperatorInstanceId, OperatorRunnable> honMap = stagelet.getOperatorMap();

        List<Endpoint> endpointList = new ArrayList<Endpoint>();

        for (ActivityNodeId hanId : activities) {
            IActivityNode han = plan.getActivityNodeMap().get(hanId);
            if (LOGGER.isLoggable(Level.FINEST)) {
                LOGGER.finest("Initializing " + hanId + " -> " + han);
            }
            IOperatorDescriptor op = han.getOwner();
            List<IConnectorDescriptor> inputs = plan.getTaskInputs(hanId);
            String[] partitions = op.getPartitions();
            for (int i = 0; i < partitions.length; ++i) {
                String part = partitions[i];
                if (id.equals(part)) {
                    IOperatorNodePushable hon = han.createPushRuntime(ctx, plan, joblet.getEnvironment(op, i), i);
                    OperatorRunnable or = new OperatorRunnable(ctx, hon);
                    stagelet.setOperator(op.getOperatorId(), i, or);
                    if (inputs != null) {
                        for (int j = 0; j < inputs.size(); ++j) {
                            if (j >= 1) {
                                throw new IllegalStateException();
                            }
                            IConnectorDescriptor conn = inputs.get(j);
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
                            IFrameReader reader = createReader(conn, drlf, i, plan, stagelet);
                            or.setFrameReader(reader);
                        }
                    }
                    honMap.put(new OperatorInstanceId(op.getOperatorId(), i), or);
                }
            }
        }

        stagelet.setEndpointList(endpointList);

        return portMap;
    }

    private IFrameReader createReader(final IConnectorDescriptor conn, IConnectionDemultiplexer demux,
        final int receiverIndex, JobPlan plan, final Stagelet stagelet) throws HyracksDataException {
        final IFrameReader reader = conn.createReceiveSideReader(ctx, plan, demux, receiverIndex);

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
                stagelet.getStatistics().getStatisticsMap().put(
                    "framecount." + conn.getConnectorId().getId() + ".receiver." + receiverIndex,
                    String.valueOf(frameCount));
            }
        } : reader;
    }

    @Override
    public void initializeJobletPhase2(UUID jobId, final JobPlan plan, UUID stageId, Set<ActivityNodeId> activities,
        final Map<PortInstanceId, Endpoint> globalPortMap) throws Exception {
        LOGGER.log(Level.INFO, String.valueOf(jobId) + "[" + id + ":" + stageId + "]: Initializing Joblet Phase 2");
        final Joblet ji = getLocalJoblet(jobId);
        Stagelet si = (Stagelet) ji.getStagelet(stageId);
        final Map<OperatorInstanceId, OperatorRunnable> honMap = si.getOperatorMap();

        final Stagelet stagelet = (Stagelet) ji.getStagelet(stageId);

        final JobSpecification spec = plan.getJobSpecification();

        for (ActivityNodeId hanId : activities) {
            IActivityNode han = plan.getActivityNodeMap().get(hanId);
            IOperatorDescriptor op = han.getOwner();
            List<IConnectorDescriptor> outputs = plan.getTaskOutputs(hanId);
            String[] partitions = op.getPartitions();
            for (int i = 0; i < partitions.length; ++i) {
                String part = partitions[i];
                if (id.equals(part)) {
                    OperatorRunnable or = honMap.get(new OperatorInstanceId(op.getOperatorId(), i));
                    if (outputs != null) {
                        for (int j = 0; j < outputs.size(); ++j) {
                            final IConnectorDescriptor conn = outputs.get(j);
                            final int senderIndex = i;
                            IEndpointDataWriterFactory edwFactory = new IEndpointDataWriterFactory() {
                                @Override
                                public IFrameWriter createFrameWriter(int index) throws HyracksDataException {
                                    PortInstanceId piId = new PortInstanceId(spec.getConsumer(conn).getOperatorId(),
                                        Direction.INPUT, spec.getConsumerInputIndex(conn), index);
                                    Endpoint ep = globalPortMap.get(piId);
                                    if (LOGGER.isLoggable(Level.FINEST)) {
                                        LOGGER.finest("Probed endpoint " + piId + " -> " + ep);
                                    }
                                    return createWriter(connectionManager.connect(ep.getNetworkAddress(), ep
                                        .getEndpointId(), senderIndex), plan, conn, senderIndex, index, stagelet);
                                }
                            };
                            or.setFrameWriter(j, conn.createSendSideWriter(ctx, plan, edwFactory, i));
                        }
                    }
                    stagelet.installRunnable(new OperatorInstanceId(op.getOperatorId(), i));
                }
            }
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
                stagelet.getStatistics().getStatisticsMap().put(
                    "framecount." + conn.getConnectorId().getId() + ".sender." + senderIndex + "." + receiverIndex,
                    String.valueOf(frameCount));
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
        ccs.notifyStageletComplete(jobId, stageId, attempt, id, stats);
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
}