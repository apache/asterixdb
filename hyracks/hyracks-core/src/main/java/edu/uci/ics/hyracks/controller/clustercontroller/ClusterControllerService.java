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
package edu.uci.ics.hyracks.controller.clustercontroller;

import java.io.IOException;
import java.io.PrintWriter;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import jol.core.Runtime;
import jol.core.Runtime.DebugLevel;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandler;

import edu.uci.ics.hyracks.api.comm.Endpoint;
import edu.uci.ics.hyracks.api.controller.IClusterController;
import edu.uci.ics.hyracks.api.controller.INodeController;
import edu.uci.ics.hyracks.api.controller.NodeParameters;
import edu.uci.ics.hyracks.api.dataflow.ActivityNodeId;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.PortInstanceId;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobPlan;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.api.job.statistics.JobStatistics;
import edu.uci.ics.hyracks.api.job.statistics.StageletStatistics;
import edu.uci.ics.hyracks.config.CCConfig;
import edu.uci.ics.hyracks.controller.AbstractRemoteService;
import edu.uci.ics.hyracks.web.WebServer;

public class ClusterControllerService extends AbstractRemoteService implements IClusterController {
    private static final long serialVersionUID = 1L;

    private CCConfig ccConfig;

    private static Logger LOGGER = Logger.getLogger(ClusterControllerService.class.getName());

    private final Map<String, NodeControllerState> nodeRegistry;

    private WebServer webServer;

    private final IJobManager jobManager;

    private final Executor taskExecutor;

    private final Timer timer;

    private Runtime jolRuntime;

    public ClusterControllerService(CCConfig ccConfig) throws Exception {
        this.ccConfig = ccConfig;
        nodeRegistry = new LinkedHashMap<String, NodeControllerState>();
        Set<DebugLevel> jolDebugLevel = LOGGER.isLoggable(Level.FINE) ? Runtime.DEBUG_ALL : new HashSet<DebugLevel>();
        jolRuntime = (Runtime) Runtime.create(jolDebugLevel, System.err);
        jobManager = new JOLJobManagerImpl(this, jolRuntime);
        taskExecutor = Executors.newCachedThreadPool();
        webServer = new WebServer(new Handler[] { getAdminConsoleHandler(), getApplicationDataHandler() });
        this.timer = new Timer(true);
    }

    @Override
    public void start() throws Exception {
        LOGGER.log(Level.INFO, "Starting ClusterControllerService");
        Registry registry = LocateRegistry.createRegistry(ccConfig.port);
        registry.rebind(IClusterController.class.getName(), this);
        webServer.setPort(ccConfig.httpPort);
        webServer.start();
        timer.schedule(new DeadNodeSweeper(), 0, ccConfig.heartbeatPeriod);
        LOGGER.log(Level.INFO, "Started ClusterControllerService");
    }

    @Override
    public void stop() throws Exception {
        LOGGER.log(Level.INFO, "Stopping ClusterControllerService");
        webServer.stop();
        LOGGER.log(Level.INFO, "Stopped ClusterControllerService");
    }

    @Override
    public UUID createJob(JobSpecification jobSpec) throws Exception {
        return jobManager.createJob(jobSpec, EnumSet.noneOf(JobFlag.class));
    }

    @Override
    public UUID createJob(JobSpecification jobSpec, EnumSet<JobFlag> jobFlags) throws Exception {
        return jobManager.createJob(jobSpec, jobFlags);
    }

    @Override
    public NodeParameters registerNode(INodeController nodeController) throws Exception {
        String id = nodeController.getId();
        NodeControllerState state = new NodeControllerState(nodeController);
        synchronized (this) {
            if (nodeRegistry.containsKey(id)) {
                throw new Exception("Node with this name already registered.");
            }
            nodeRegistry.put(id, state);
        }
        nodeController.notifyRegistration(this);
        jobManager.registerNode(id);
        LOGGER.log(Level.INFO, "Registered INodeController: id = " + id);
        NodeParameters params = new NodeParameters();
        params.setHeartbeatPeriod(ccConfig.heartbeatPeriod);
        return params;
    }

    @Override
    public void unregisterNode(INodeController nodeController) throws Exception {
        String id = nodeController.getId();
        synchronized (this) {
            nodeRegistry.remove(id);
        }
        LOGGER.log(Level.INFO, "Unregistered INodeController");
    }

    public synchronized NodeControllerState lookupNode(String id) throws Exception {
        return nodeRegistry.get(id);
    }

    public Executor getExecutor() {
        return taskExecutor;
    }

    public synchronized void notifyJobComplete(final UUID jobId) {
        for (final NodeControllerState ns : nodeRegistry.values()) {
            taskExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        ns.getNodeController().cleanUpJob(jobId);
                    } catch (Exception e) {
                    }
                }

            });
        }
    }

    @Override
    public void notifyStageletComplete(UUID jobId, UUID stageId, int attempt, String nodeId,
            StageletStatistics statistics) throws Exception {
        jobManager.notifyStageletComplete(jobId, stageId, attempt, nodeId, statistics);
    }

    @Override
    public void notifyStageletFailure(UUID jobId, UUID stageId, int attempt, String nodeId) throws Exception {
        jobManager.notifyStageletFailure(jobId, stageId, attempt, nodeId);
    }

    @Override
    public JobStatus getJobStatus(UUID jobId) throws Exception {
        return jobManager.getJobStatus(jobId);
    }

    @Override
    public void start(UUID jobId) throws Exception {
        jobManager.start(jobId);
    }

    @Override
    public JobStatistics waitForCompletion(UUID jobId) throws Exception {
        return jobManager.waitForCompletion(jobId);
    }

    private Handler getAdminConsoleHandler() {
        ContextHandler handler = new ContextHandler("/admin");
        handler.setHandler(new AbstractHandler() {
            @Override
            public void handle(String target, Request baseRequest, HttpServletRequest request,
                    HttpServletResponse response) throws IOException, ServletException {
                if (!"/".equals(target)) {
                    return;
                }
                response.setContentType("text/html;charset=utf-8");
                response.setStatus(HttpServletResponse.SC_OK);
                baseRequest.setHandled(true);
                PrintWriter writer = response.getWriter();
                writer.println("<html><head><title>Hyracks Admin Console</title></head><body>");
                writer.println("<h1>Hyracks Admin Console</h1>");
                writer.println("<h2>Node Controllers</h2>");
                writer.println("<table><tr><td>Node Id</td><td>Host</td></tr>");
                synchronized (ClusterControllerService.this) {
                    for (Map.Entry<String, NodeControllerState> e : nodeRegistry.entrySet()) {
                        try {
                            writer.print("<tr><td>");
                            writer.print(e.getKey());
                            writer.print("</td><td>");
                            writer.print(e.getValue().getLastHeartbeatDuration());
                            writer.print("</td></tr>");
                        } catch (Exception ex) {
                        }
                    }
                }
                writer.println("</table>");
                writer.println("</body></html>");
                writer.flush();
            }
        });
        return handler;
    }

    private Handler getApplicationDataHandler() {
        ContextHandler handler = new ContextHandler("/applications");
        handler.setHandler(new AbstractHandler() {
            @Override
            public void handle(String target, Request baseRequest, HttpServletRequest request,
                    HttpServletResponse response) throws IOException, ServletException {
            }
        });
        return handler;
    }

    @Override
    public Map<String, INodeController> getRegistry() throws Exception {
        Map<String, INodeController> map = new HashMap<String, INodeController>();
        for (Map.Entry<String, NodeControllerState> e : nodeRegistry.entrySet()) {
            map.put(e.getKey(), e.getValue().getNodeController());
        }
        return map;
    }

    @Override
    public synchronized void nodeHeartbeat(String id) throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Heartbeat from: " + id);
        }
        NodeControllerState ns = nodeRegistry.get(id);
        if (ns != null) {
            ns.notifyHeartbeat();
        }
    }

    private void killNode(String nodeId) throws Exception {
        nodeRegistry.remove(nodeId);
        jobManager.notifyNodeFailure(nodeId);
    }

    private class DeadNodeSweeper extends TimerTask {
        @Override
        public void run() {
            Set<String> deadNodes = new HashSet<String>();
            synchronized (ClusterControllerService.this) {
                for (Map.Entry<String, NodeControllerState> e : nodeRegistry.entrySet()) {
                    NodeControllerState state = e.getValue();
                    if (state.incrementLastHeartbeatDuration() >= ccConfig.maxHeartbeatLapsePeriods) {
                        deadNodes.add(e.getKey());
                    }
                }
                for (String deadNode : deadNodes) {
                    try {
                        if (LOGGER.isLoggable(Level.INFO)) {
                            LOGGER.info("Killing node: " + deadNode);
                        }
                        killNode(deadNode);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    interface RemoteOp<T> {
        public String getNodeId();

        public T execute(INodeController node) throws Exception;
    }

    interface Accumulator<T, R> {
        public void accumulate(T o);

        public R getResult();
    }

    <T, R> R runRemote(final RemoteOp<T>[] remoteOps, final Accumulator<T, R> accumulator) throws Exception {
        final Semaphore installComplete = new Semaphore(remoteOps.length);
        final List<Exception> errors = new Vector<Exception>();
        for (final RemoteOp<T> remoteOp : remoteOps) {
            NodeControllerState nodeState = lookupNode(remoteOp.getNodeId());
            final INodeController node = nodeState.getNodeController();

            installComplete.acquire();
            Runnable remoteRunner = new Runnable() {
                @Override
                public void run() {
                    try {
                        T t = remoteOp.execute(node);
                        if (accumulator != null) {
                            synchronized (accumulator) {
                                accumulator.accumulate(t);
                            }
                        }
                    } catch (Exception e) {
                        errors.add(e);
                    } finally {
                        installComplete.release();
                    }
                }
            };

            getExecutor().execute(remoteRunner);
        }
        installComplete.acquire(remoteOps.length);
        if (!errors.isEmpty()) {
            throw errors.get(0);
        }
        return accumulator == null ? null : accumulator.getResult();
    }

    static class Phase1Installer implements RemoteOp<Map<PortInstanceId, Endpoint>> {
        private String nodeId;
        private UUID jobId;
        private JobPlan plan;
        private UUID stageId;
        private int attempt;
        private Map<ActivityNodeId, Set<Integer>> tasks;
        private Map<OperatorDescriptorId, Set<Integer>> opPartitions;

        public Phase1Installer(String nodeId, UUID jobId, JobPlan plan, UUID stageId, int attempt,
                Map<ActivityNodeId, Set<Integer>> tasks, Map<OperatorDescriptorId, Set<Integer>> opPartitions) {
            this.nodeId = nodeId;
            this.jobId = jobId;
            this.plan = plan;
            this.stageId = stageId;
            this.attempt = attempt;
            this.tasks = tasks;
            this.opPartitions = opPartitions;
        }

        @Override
        public Map<PortInstanceId, Endpoint> execute(INodeController node) throws Exception {
            return node.initializeJobletPhase1(jobId, plan, stageId, attempt, tasks, opPartitions);
        }

        @Override
        public String toString() {
            return jobId + " Distribution Phase 1";
        }

        @Override
        public String getNodeId() {
            return nodeId;
        }
    }

    static class Phase2Installer implements RemoteOp<Void> {
        private String nodeId;
        private UUID jobId;
        private JobPlan plan;
        private UUID stageId;
        private Map<ActivityNodeId, Set<Integer>> tasks;
        private Map<OperatorDescriptorId, Set<Integer>> opPartitions;
        private Map<PortInstanceId, Endpoint> globalPortMap;

        public Phase2Installer(String nodeId, UUID jobId, JobPlan plan, UUID stageId,
                Map<ActivityNodeId, Set<Integer>> tasks, Map<OperatorDescriptorId, Set<Integer>> opPartitions,
                Map<PortInstanceId, Endpoint> globalPortMap) {
            this.nodeId = nodeId;
            this.jobId = jobId;
            this.plan = plan;
            this.stageId = stageId;
            this.tasks = tasks;
            this.opPartitions = opPartitions;
            this.globalPortMap = globalPortMap;
        }

        @Override
        public Void execute(INodeController node) throws Exception {
            node.initializeJobletPhase2(jobId, plan, stageId, tasks, opPartitions, globalPortMap);
            return null;
        }

        @Override
        public String toString() {
            return jobId + " Distribution Phase 2";
        }

        @Override
        public String getNodeId() {
            return nodeId;
        }
    }

    static class Phase3Installer implements RemoteOp<Void> {
        private String nodeId;
        private UUID jobId;
        private UUID stageId;

        public Phase3Installer(String nodeId, UUID jobId, UUID stageId) {
            this.nodeId = nodeId;
            this.jobId = jobId;
            this.stageId = stageId;
        }

        @Override
        public Void execute(INodeController node) throws Exception {
            node.commitJobletInitialization(jobId, stageId);
            return null;
        }

        @Override
        public String toString() {
            return jobId + " Distribution Phase 3";
        }

        @Override
        public String getNodeId() {
            return nodeId;
        }
    }

    static class StageStarter implements RemoteOp<Void> {
        private String nodeId;
        private UUID jobId;
        private UUID stageId;

        public StageStarter(String nodeId, UUID jobId, UUID stageId) {
            this.nodeId = nodeId;
            this.jobId = jobId;
            this.stageId = stageId;
        }

        @Override
        public Void execute(INodeController node) throws Exception {
            node.startStage(jobId, stageId);
            return null;
        }

        @Override
        public String toString() {
            return jobId + " Started Stage: " + stageId;
        }

        @Override
        public String getNodeId() {
            return nodeId;
        }
    }

    static class JobletAborter implements RemoteOp<Void> {
        private String nodeId;
        private UUID jobId;
        private UUID stageId;

        public JobletAborter(String nodeId, UUID jobId, UUID stageId, int attempt) {
            this.nodeId = nodeId;
            this.jobId = jobId;
            this.stageId = stageId;
        }

        @Override
        public Void execute(INodeController node) throws Exception {
            node.abortJoblet(jobId, stageId);
            return null;
        }

        @Override
        public String toString() {
            return jobId + " Aborting";
        }

        @Override
        public String getNodeId() {
            return nodeId;
        }
    }

    static class JobCompleteNotifier implements RemoteOp<Void> {
        private String nodeId;
        private UUID jobId;

        public JobCompleteNotifier(String nodeId, UUID jobId) {
            this.nodeId = nodeId;
            this.jobId = jobId;
        }

        @Override
        public Void execute(INodeController node) throws Exception {
            node.cleanUpJob(jobId);
            return null;
        }

        @Override
        public String toString() {
            return jobId + " Cleaning Up";
        }

        @Override
        public String getNodeId() {
            return nodeId;
        }
    }

    static class PortMapMergingAccumulator implements
            Accumulator<Map<PortInstanceId, Endpoint>, Map<PortInstanceId, Endpoint>> {
        Map<PortInstanceId, Endpoint> portMap = new HashMap<PortInstanceId, Endpoint>();

        @Override
        public void accumulate(Map<PortInstanceId, Endpoint> o) {
            portMap.putAll(o);
        }

        @Override
        public Map<PortInstanceId, Endpoint> getResult() {
            return portMap;
        }
    }

    @Override
    public void createApplication(String appName) throws Exception {

    }

    @Override
    public void destroyApplication(String appName) throws Exception {

    }

    @Override
    public void startApplication(String appName) throws Exception {

    }
}