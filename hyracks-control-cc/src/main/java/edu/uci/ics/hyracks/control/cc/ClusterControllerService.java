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
package edu.uci.ics.hyracks.control.cc;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
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

import org.apache.commons.io.IOUtils;
import org.eclipse.jetty.http.HttpMethods;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.json.JSONObject;

import edu.uci.ics.hyracks.api.client.ClusterControllerInfo;
import edu.uci.ics.hyracks.api.client.IHyracksClientInterface;
import edu.uci.ics.hyracks.api.comm.Endpoint;
import edu.uci.ics.hyracks.api.control.CCConfig;
import edu.uci.ics.hyracks.api.control.IClusterController;
import edu.uci.ics.hyracks.api.control.INodeController;
import edu.uci.ics.hyracks.api.control.NodeParameters;
import edu.uci.ics.hyracks.api.dataflow.ActivityNodeId;
import edu.uci.ics.hyracks.api.dataflow.OperatorDescriptorId;
import edu.uci.ics.hyracks.api.dataflow.PortInstanceId;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobPlan;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.control.cc.application.CCApplicationContext;
import edu.uci.ics.hyracks.control.cc.web.WebServer;
import edu.uci.ics.hyracks.control.cc.web.handlers.util.IJSONOutputFunction;
import edu.uci.ics.hyracks.control.cc.web.handlers.util.JSONOutputRequestHandler;
import edu.uci.ics.hyracks.control.cc.web.handlers.util.RoutingHandler;
import edu.uci.ics.hyracks.control.common.AbstractRemoteService;
import edu.uci.ics.hyracks.control.common.application.ApplicationContext;
import edu.uci.ics.hyracks.control.common.context.ServerContext;

public class ClusterControllerService extends AbstractRemoteService implements IClusterController,
        IHyracksClientInterface {
    private static final long serialVersionUID = 1L;

    private CCConfig ccConfig;

    private static Logger LOGGER = Logger.getLogger(ClusterControllerService.class.getName());

    private final Map<String, NodeControllerState> nodeRegistry;

    private final Map<String, CCApplicationContext> applications;

    private final ServerContext serverCtx;

    private WebServer webServer;

    private ClusterControllerInfo info;

    private final IJobManager jobManager;

    private final Executor taskExecutor;

    private final Timer timer;

    private Runtime jolRuntime;

    public ClusterControllerService(CCConfig ccConfig) throws Exception {
        this.ccConfig = ccConfig;
        nodeRegistry = new LinkedHashMap<String, NodeControllerState>();
        applications = new Hashtable<String, CCApplicationContext>();
        serverCtx = new ServerContext(ServerContext.ServerType.CLUSTER_CONTROLLER, new File(
                ClusterControllerService.class.getName()));
        Set<DebugLevel> jolDebugLevel = LOGGER.isLoggable(Level.FINE) ? Runtime.DEBUG_ALL : new HashSet<DebugLevel>();
        jolRuntime = (Runtime) Runtime.create(jolDebugLevel, System.err);
        jobManager = new JOLJobManagerImpl(this, jolRuntime);
        taskExecutor = Executors.newCachedThreadPool();
        webServer = new WebServer();
        webServer.addHandler(getAdminConsoleHandler());
        webServer.addHandler(getApplicationInstallationHandler());
        webServer.addHandler(getRestAPIHandler());
        this.timer = new Timer(true);
    }

    @Override
    public void start() throws Exception {
        LOGGER.log(Level.INFO, "Starting ClusterControllerService");
        Registry registry = LocateRegistry.createRegistry(ccConfig.port);
        registry.rebind(IHyracksClientInterface.class.getName(), this);
        registry.rebind(IClusterController.class.getName(), this);
        webServer.setPort(ccConfig.httpPort);
        webServer.start();
        info = new ClusterControllerInfo();
        info.setWebPort(webServer.getListeningPort());
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
    public UUID createJob(String appName, byte[] jobSpec, EnumSet<JobFlag> jobFlags) throws Exception {
        CCApplicationContext appCtx = getApplicationContext(appName);
        if (appCtx == null) {
            throw new HyracksException("No application with id " + appName + " found");
        }
        UUID jobId = UUID.randomUUID();
        JobSpecification specification = appCtx.createJobSpecification(jobId, jobSpec);
        jobManager.createJob(jobId, appName, specification, jobFlags);
        appCtx.notifyJobCreation(jobId, specification);
        return jobId;
    }

    public synchronized CCApplicationContext getApplicationContext(String appName) {
        return applications.get(appName);
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
        params.setClusterControllerInfo(info);
        params.setHeartbeatPeriod(ccConfig.heartbeatPeriod);
        params.setProfileDumpPeriod(ccConfig.profileDumpPeriod);
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
            Map<String, Long> statistics) throws Exception {
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
    public void waitForCompletion(UUID jobId) throws Exception {
        jobManager.waitForCompletion(jobId);
    }

    @Override
    public void reportProfile(String id, Map<UUID, Map<String, Long>> counterDump) throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Profile: " + id + ": " + counterDump);
        }
        jobManager.reportProfile(id, counterDump);
    }

    private Handler getRestAPIHandler() {
        ContextHandler handler = new ContextHandler("/state");
        RoutingHandler rh = new RoutingHandler();
        rh.addHandler("jobs", new JSONOutputRequestHandler(new IJSONOutputFunction() {
            @Override
            public JSONObject invoke(String[] arguments) throws Exception {
                JSONObject result = new JSONObject();
                switch (arguments.length) {
                    case 1:
                        if (!"".equals(arguments[0])) {
                            break;
                        }
                    case 0:
                        result.put("result", jobManager.getQueryInterface().getAllJobSummaries());
                        break;

                    case 2:
                        UUID jobId = UUID.fromString(arguments[0]);

                        if ("spec".equalsIgnoreCase(arguments[1])) {
                            result.put("result", jobManager.getQueryInterface().getJobSpecification(jobId));
                        } else if ("profile".equalsIgnoreCase(arguments[1])) {
                            result.put("result", jobManager.getQueryInterface().getJobProfile(jobId));
                        }
                }
                return result;
            }
        }));
        handler.setHandler(rh);
        return handler;
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

    private Handler getApplicationInstallationHandler() {
        ContextHandler handler = new ContextHandler("/applications");
        handler.setHandler(new AbstractHandler() {
            @Override
            public void handle(String target, Request baseRequest, HttpServletRequest request,
                    HttpServletResponse response) throws IOException, ServletException {
                try {
                    while (target.startsWith("/")) {
                        target = target.substring(1);
                    }
                    while (target.endsWith("/")) {
                        target = target.substring(0, target.length() - 1);
                    }
                    String[] parts = target.split("/");
                    if (parts.length != 1) {
                        return;
                    }
                    String appName = parts[0];
                    ApplicationContext appCtx;
                    appCtx = getApplicationContext(appName);
                    if (appCtx != null) {
                        if (HttpMethods.PUT.equals(request.getMethod())) {
                            OutputStream os = appCtx.getHarOutputStream();
                            try {
                                IOUtils.copyLarge(request.getInputStream(), os);
                            } finally {
                                os.close();
                            }
                        } else if (HttpMethods.GET.equals(request.getMethod())) {
                            if (!appCtx.containsHar()) {
                                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                            } else {
                                InputStream is = appCtx.getHarInputStream();
                                response.setContentType("application/octet-stream");
                                response.setStatus(HttpServletResponse.SC_OK);
                                try {
                                    IOUtils.copyLarge(is, response.getOutputStream());
                                } finally {
                                    is.close();
                                }
                            }
                        }
                        baseRequest.setHandled(true);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    throw e;
                }
            }
        });
        return handler;
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

    @Override
    public void createApplication(String appName) throws Exception {
        synchronized (applications) {
            if (applications.containsKey(appName)) {
                throw new HyracksException("Duplicate application with name: " + appName + " being created.");
            }
            CCApplicationContext appCtx = new CCApplicationContext(serverCtx, appName);
            applications.put(appName, appCtx);
        }
    }

    @Override
    public void destroyApplication(String appName) throws Exception {
        ApplicationContext appCtx = applications.remove(appName);
        if (appCtx != null) {
            synchronized (this) {
                for (NodeControllerState ncs : nodeRegistry.values()) {
                    ncs.getNodeController().destroyApplication(appName);
                }
            }
            appCtx.deinitialize();
        }
    }

    @Override
    public void startApplication(final String appName) throws Exception {
        ApplicationContext appCtx = getApplicationContext(appName);
        appCtx.initializeClassPath();
        appCtx.initialize();
        final byte[] serializedDistributedState = JavaSerializationUtils.serialize(appCtx.getDestributedState());
        final boolean deployHar = appCtx.containsHar();
        RemoteOp<Void>[] ops;
        synchronized (this) {
            List<RemoteOp<Void>> opList = new ArrayList<RemoteOp<Void>>();
            for (final String nodeId : nodeRegistry.keySet()) {
                opList.add(new RemoteOp<Void>() {
                    @Override
                    public String getNodeId() {
                        return nodeId;
                    }

                    @Override
                    public Void execute(INodeController node) throws Exception {
                        node.createApplication(appName, deployHar, serializedDistributedState);
                        return null;
                    }
                });
            }
            ops = opList.toArray(new RemoteOp[opList.size()]);
        }
        runRemote(ops, null);
    }

    @Override
    public ClusterControllerInfo getClusterControllerInfo() throws Exception {
        return info;
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

    private static byte[] serialize(Object o) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(o);
        return baos.toByteArray();
    }

    static class Phase1Installer implements RemoteOp<Map<PortInstanceId, Endpoint>> {
        private String nodeId;
        private UUID jobId;
        private String appName;
        private JobPlan plan;
        private UUID stageId;
        private int attempt;
        private Map<ActivityNodeId, Set<Integer>> tasks;
        private Map<OperatorDescriptorId, Set<Integer>> opPartitions;

        public Phase1Installer(String nodeId, UUID jobId, String appName, JobPlan plan, UUID stageId, int attempt,
                Map<ActivityNodeId, Set<Integer>> tasks, Map<OperatorDescriptorId, Set<Integer>> opPartitions) {
            this.nodeId = nodeId;
            this.jobId = jobId;
            this.appName = appName;
            this.plan = plan;
            this.stageId = stageId;
            this.attempt = attempt;
            this.tasks = tasks;
            this.opPartitions = opPartitions;
        }

        @Override
        public Map<PortInstanceId, Endpoint> execute(INodeController node) throws Exception {
            return node.initializeJobletPhase1(appName, jobId, attempt, serialize(plan), stageId, tasks, opPartitions);
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
        private String appName;
        private JobPlan plan;
        private UUID stageId;
        private Map<ActivityNodeId, Set<Integer>> tasks;
        private Map<OperatorDescriptorId, Set<Integer>> opPartitions;
        private Map<PortInstanceId, Endpoint> globalPortMap;

        public Phase2Installer(String nodeId, UUID jobId, String appName, JobPlan plan, UUID stageId,
                Map<ActivityNodeId, Set<Integer>> tasks, Map<OperatorDescriptorId, Set<Integer>> opPartitions,
                Map<PortInstanceId, Endpoint> globalPortMap) {
            this.nodeId = nodeId;
            this.jobId = jobId;
            this.appName = appName;
            this.plan = plan;
            this.stageId = stageId;
            this.tasks = tasks;
            this.opPartitions = opPartitions;
            this.globalPortMap = globalPortMap;
        }

        @Override
        public Void execute(INodeController node) throws Exception {
            node.initializeJobletPhase2(appName, jobId, serialize(plan), stageId, tasks, opPartitions, globalPortMap);
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
}