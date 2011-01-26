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

import java.io.File;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.client.ClusterControllerInfo;
import edu.uci.ics.hyracks.api.client.IHyracksClientInterface;
import edu.uci.ics.hyracks.api.control.CCConfig;
import edu.uci.ics.hyracks.api.control.IClusterController;
import edu.uci.ics.hyracks.api.control.INodeController;
import edu.uci.ics.hyracks.api.control.NodeParameters;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.api.job.profiling.om.JobProfile;
import edu.uci.ics.hyracks.api.job.profiling.om.StageletProfile;
import edu.uci.ics.hyracks.control.cc.application.CCApplicationContext;
import edu.uci.ics.hyracks.control.cc.job.IJobStatusConditionVariable;
import edu.uci.ics.hyracks.control.cc.job.JobRun;
import edu.uci.ics.hyracks.control.cc.job.manager.events.ApplicationDestroyEvent;
import edu.uci.ics.hyracks.control.cc.job.manager.events.ApplicationStartEvent;
import edu.uci.ics.hyracks.control.cc.job.manager.events.GetJobStatusConditionVariableEvent;
import edu.uci.ics.hyracks.control.cc.job.manager.events.GetJobStatusEvent;
import edu.uci.ics.hyracks.control.cc.job.manager.events.JobCreateEvent;
import edu.uci.ics.hyracks.control.cc.job.manager.events.JobStartEvent;
import edu.uci.ics.hyracks.control.cc.job.manager.events.NodeHeartbeatEvent;
import edu.uci.ics.hyracks.control.cc.job.manager.events.RegisterNodeEvent;
import edu.uci.ics.hyracks.control.cc.job.manager.events.RemoveDeadNodesEvent;
import edu.uci.ics.hyracks.control.cc.job.manager.events.ReportProfilesEvent;
import edu.uci.ics.hyracks.control.cc.job.manager.events.StageletCompleteEvent;
import edu.uci.ics.hyracks.control.cc.job.manager.events.StageletFailureEvent;
import edu.uci.ics.hyracks.control.cc.job.manager.events.UnregisterNodeEvent;
import edu.uci.ics.hyracks.control.cc.jobqueue.FutureValue;
import edu.uci.ics.hyracks.control.cc.jobqueue.JobQueue;
import edu.uci.ics.hyracks.control.cc.scheduler.IScheduler;
import edu.uci.ics.hyracks.control.cc.scheduler.naive.NaiveScheduler;
import edu.uci.ics.hyracks.control.cc.web.WebServer;
import edu.uci.ics.hyracks.control.common.AbstractRemoteService;
import edu.uci.ics.hyracks.control.common.context.ServerContext;

public class ClusterControllerService extends AbstractRemoteService implements IClusterController,
        IHyracksClientInterface {
    private static final long serialVersionUID = 1L;

    private CCConfig ccConfig;

    private static Logger LOGGER = Logger.getLogger(ClusterControllerService.class.getName());

    private final Map<String, NodeControllerState> nodeRegistry;

    private final Map<String, CCApplicationContext> applications;

    private final ServerContext serverCtx;

    private final WebServer webServer;

    private ClusterControllerInfo info;

    private final Map<UUID, JobRun> runMap;

    private final JobQueue jobQueue;

    private final IScheduler scheduler;

    private final Executor taskExecutor;

    private final Timer timer;

    public ClusterControllerService(CCConfig ccConfig) throws Exception {
        this.ccConfig = ccConfig;
        nodeRegistry = new LinkedHashMap<String, NodeControllerState>();
        applications = new Hashtable<String, CCApplicationContext>();
        serverCtx = new ServerContext(ServerContext.ServerType.CLUSTER_CONTROLLER, new File(
                ClusterControllerService.class.getName()));
        taskExecutor = Executors.newCachedThreadPool();
        webServer = new WebServer(this);
        runMap = new HashMap<UUID, JobRun>();
        jobQueue = new JobQueue();
        scheduler = new NaiveScheduler(this);
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

    public Map<String, CCApplicationContext> getApplicationMap() {
        return applications;
    }

    public Map<UUID, JobRun> getRunMap() {
        return runMap;
    }

    public JobQueue getJobQueue() {
        return jobQueue;
    }

    public IScheduler getScheduler() {
        return scheduler;
    }

    public Executor getExecutor() {
        return taskExecutor;
    }

    public Map<String, NodeControllerState> getNodeMap() {
        return nodeRegistry;
    }

    public CCConfig getConfig() {
        return ccConfig;
    }

    @Override
    public UUID createJob(String appName, byte[] jobSpec, EnumSet<JobFlag> jobFlags) throws Exception {
        UUID jobId = UUID.randomUUID();
        JobCreateEvent jce = new JobCreateEvent(this, jobId, appName, jobSpec, jobFlags);
        jobQueue.schedule(jce);
        jce.sync();
        return jobId;
    }

    @Override
    public NodeParameters registerNode(INodeController nodeController) throws Exception {
        String id = nodeController.getId();
        NodeControllerState state = new NodeControllerState(nodeController);
        jobQueue.scheduleAndSync(new RegisterNodeEvent(this, id, state));
        nodeController.notifyRegistration(this);
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
        jobQueue.scheduleAndSync(new UnregisterNodeEvent(this, id));
        LOGGER.log(Level.INFO, "Unregistered INodeController");
    }

    @Override
    public void notifyStageletComplete(UUID jobId, UUID stageId, int attempt, String nodeId, StageletProfile statistics)
            throws Exception {
        StageletCompleteEvent sce = new StageletCompleteEvent(this, jobId, stageId, attempt, nodeId);
        jobQueue.schedule(sce);
    }

    @Override
    public void notifyStageletFailure(UUID jobId, UUID stageId, int attempt, String nodeId) throws Exception {
        StageletFailureEvent sfe = new StageletFailureEvent(this, jobId, stageId, attempt, nodeId);
        jobQueue.schedule(sfe);
    }

    @Override
    public JobStatus getJobStatus(UUID jobId) throws Exception {
        GetJobStatusEvent gse = new GetJobStatusEvent(this, jobId);
        jobQueue.scheduleAndSync(gse);
        return gse.getStatus();
    }

    @Override
    public void start(UUID jobId) throws Exception {
        JobStartEvent jse = new JobStartEvent(this, jobId);
        jobQueue.scheduleAndSync(jse);
    }

    @Override
    public void waitForCompletion(UUID jobId) throws Exception {
        GetJobStatusConditionVariableEvent e = new GetJobStatusConditionVariableEvent(this, jobId);
        jobQueue.scheduleAndSync(e);
        IJobStatusConditionVariable var = e.getConditionVariable();
        if (var != null) {
            var.waitForCompletion();
        }
    }

    @Override
    public void reportProfile(String id, List<JobProfile> profiles) throws Exception {
        jobQueue.schedule(new ReportProfilesEvent(this, profiles));
    }

    @Override
    public synchronized void nodeHeartbeat(String id) throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Heartbeat from: " + id);
        }
        jobQueue.schedule(new NodeHeartbeatEvent(this, id));
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
        FutureValue fv = new FutureValue();
        jobQueue.schedule(new ApplicationDestroyEvent(this, appName, fv));
        fv.get();
    }

    @Override
    public void startApplication(final String appName) throws Exception {
        FutureValue fv = new FutureValue();
        jobQueue.schedule(new ApplicationStartEvent(this, appName, fv));
        fv.get();
    }

    @Override
    public ClusterControllerInfo getClusterControllerInfo() throws Exception {
        return info;
    }

    private class DeadNodeSweeper extends TimerTask {
        @Override
        public void run() {
            jobQueue.schedule(new RemoveDeadNodesEvent(ClusterControllerService.this));
        }
    }
}