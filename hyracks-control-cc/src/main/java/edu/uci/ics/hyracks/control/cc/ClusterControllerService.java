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
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.client.ClusterControllerInfo;
import edu.uci.ics.hyracks.api.client.IHyracksClientInterface;
import edu.uci.ics.hyracks.api.context.ICCContext;
import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.control.cc.application.CCApplicationContext;
import edu.uci.ics.hyracks.control.cc.job.IJobStatusConditionVariable;
import edu.uci.ics.hyracks.control.cc.job.JobRun;
import edu.uci.ics.hyracks.control.cc.web.WebServer;
import edu.uci.ics.hyracks.control.cc.work.ApplicationDestroyWork;
import edu.uci.ics.hyracks.control.cc.work.ApplicationStartWork;
import edu.uci.ics.hyracks.control.cc.work.GetJobStatusConditionVariableWork;
import edu.uci.ics.hyracks.control.cc.work.GetJobStatusWork;
import edu.uci.ics.hyracks.control.cc.work.JobCreateWork;
import edu.uci.ics.hyracks.control.cc.work.JobStartWork;
import edu.uci.ics.hyracks.control.cc.work.NodeHeartbeatWork;
import edu.uci.ics.hyracks.control.cc.work.RegisterNodeWork;
import edu.uci.ics.hyracks.control.cc.work.RegisterPartitionAvailibilityWork;
import edu.uci.ics.hyracks.control.cc.work.RegisterPartitionRequestWork;
import edu.uci.ics.hyracks.control.cc.work.RemoveDeadNodesWork;
import edu.uci.ics.hyracks.control.cc.work.ReportProfilesWork;
import edu.uci.ics.hyracks.control.cc.work.TaskCompleteWork;
import edu.uci.ics.hyracks.control.cc.work.TaskFailureWork;
import edu.uci.ics.hyracks.control.cc.work.UnregisterNodeWork;
import edu.uci.ics.hyracks.control.common.AbstractRemoteService;
import edu.uci.ics.hyracks.control.common.base.IClusterController;
import edu.uci.ics.hyracks.control.common.base.INodeController;
import edu.uci.ics.hyracks.control.common.context.ServerContext;
import edu.uci.ics.hyracks.control.common.controllers.CCConfig;
import edu.uci.ics.hyracks.control.common.controllers.NodeParameters;
import edu.uci.ics.hyracks.control.common.controllers.NodeRegistration;
import edu.uci.ics.hyracks.control.common.heartbeat.HeartbeatData;
import edu.uci.ics.hyracks.control.common.job.PartitionDescriptor;
import edu.uci.ics.hyracks.control.common.job.PartitionRequest;
import edu.uci.ics.hyracks.control.common.job.profiling.om.JobProfile;
import edu.uci.ics.hyracks.control.common.job.profiling.om.TaskProfile;
import edu.uci.ics.hyracks.control.common.logs.LogFile;
import edu.uci.ics.hyracks.control.common.work.FutureValue;
import edu.uci.ics.hyracks.control.common.work.WorkQueue;

public class ClusterControllerService extends AbstractRemoteService implements IClusterController,
        IHyracksClientInterface {
    private static final long serialVersionUID = 1L;

    private final CCConfig ccConfig;

    private static Logger LOGGER = Logger.getLogger(ClusterControllerService.class.getName());

    private final LogFile jobLog;

    private final Map<String, NodeControllerState> nodeRegistry;

    private final Map<String, Set<String>> ipAddressNodeNameMap;

    private final Map<String, CCApplicationContext> applications;

    private final ServerContext serverCtx;

    private final WebServer webServer;

    private ClusterControllerInfo info;

    private final Map<JobId, JobRun> activeRunMap;

    private final Map<JobId, JobRun> runMapArchive;

    private final WorkQueue workQueue;

    private final Executor taskExecutor;

    private final Timer timer;

    private final CCClientInterface ccci;

    private final ICCContext ccContext;

    private final DeadNodeSweeper sweeper;

    private long jobCounter;

    public ClusterControllerService(final CCConfig ccConfig) throws Exception {
        this.ccConfig = ccConfig;
        File jobLogFolder = new File(ccConfig.ccRoot, "logs/jobs");
        jobLog = new LogFile(jobLogFolder);
        nodeRegistry = new LinkedHashMap<String, NodeControllerState>();
        ipAddressNodeNameMap = new HashMap<String, Set<String>>();
        applications = new Hashtable<String, CCApplicationContext>();
        serverCtx = new ServerContext(ServerContext.ServerType.CLUSTER_CONTROLLER, new File(ccConfig.ccRoot));
        taskExecutor = Executors.newCachedThreadPool();
        webServer = new WebServer(this);
        activeRunMap = new HashMap<JobId, JobRun>();
        runMapArchive = new LinkedHashMap<JobId, JobRun>() {
            private static final long serialVersionUID = 1L;

            protected boolean removeEldestEntry(Map.Entry<JobId, JobRun> eldest) {
                return size() > ccConfig.jobHistorySize;
            }
        };
        workQueue = new WorkQueue();
        this.timer = new Timer(true);
        ccci = new CCClientInterface(this);
        ccContext = new ICCContext() {
            @Override
            public Map<String, Set<String>> getIPAddressNodeMap() {
                return ipAddressNodeNameMap;
            }
        };
        sweeper = new DeadNodeSweeper();
        jobCounter = 0;
    }

    @Override
    public void start() throws Exception {
        LOGGER.log(Level.INFO, "Starting ClusterControllerService: " + this);
        Registry registry = LocateRegistry.createRegistry(ccConfig.port);
        registry.rebind(IHyracksClientInterface.class.getName(), ccci);
        registry.rebind(IClusterController.class.getName(), this);
        webServer.setPort(ccConfig.httpPort);
        webServer.start();
        workQueue.start();
        info = new ClusterControllerInfo();
        info.setWebPort(webServer.getListeningPort());
        timer.schedule(sweeper, 0, ccConfig.heartbeatPeriod);
        jobLog.open();
        LOGGER.log(Level.INFO, "Started ClusterControllerService");
    }

    @Override
    public void stop() throws Exception {
        LOGGER.log(Level.INFO, "Stopping ClusterControllerService");
        webServer.stop();
        sweeper.cancel();
        workQueue.stop();
        jobLog.close();
        LOGGER.log(Level.INFO, "Stopped ClusterControllerService");
    }

    public Map<String, CCApplicationContext> getApplicationMap() {
        return applications;
    }

    public Map<JobId, JobRun> getActiveRunMap() {
        return activeRunMap;
    }

    public Map<JobId, JobRun> getRunMapArchive() {
        return runMapArchive;
    }

    public LogFile getJobLogFile() {
        return jobLog;
    }

    public WorkQueue getWorkQueue() {
        return workQueue;
    }

    public Executor getExecutor() {
        return taskExecutor;
    }

    public Map<String, NodeControllerState> getNodeMap() {
        return nodeRegistry;
    }

    public Map<String, Set<String>> getIPAddressNodeNameMap() {
        return ipAddressNodeNameMap;
    }

    public CCConfig getConfig() {
        return ccConfig;
    }

    private JobId createJobId() {
        return new JobId(jobCounter++);
    }

    @Override
    public JobId createJob(String appName, byte[] jobSpec, EnumSet<JobFlag> jobFlags) throws Exception {
        JobId jobId = createJobId();
        JobCreateWork jce = new JobCreateWork(this, jobId, appName, jobSpec, jobFlags);
        workQueue.schedule(jce);
        jce.sync();
        return jobId;
    }

    @Override
    public NodeParameters registerNode(NodeRegistration reg) throws Exception {
        INodeController nodeController = reg.getNodeController();
        String id = reg.getNodeId();
        NodeControllerState state = new NodeControllerState(nodeController, reg);
        workQueue.scheduleAndSync(new RegisterNodeWork(this, id, state));
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
        workQueue.scheduleAndSync(new UnregisterNodeWork(this, id));
        LOGGER.log(Level.INFO, "Unregistered INodeController");
    }

    @Override
    public void notifyTaskComplete(JobId jobId, TaskAttemptId taskId, String nodeId, TaskProfile statistics)
            throws Exception {
        TaskCompleteWork sce = new TaskCompleteWork(this, jobId, taskId, nodeId, statistics);
        workQueue.schedule(sce);
    }

    @Override
    public void notifyTaskFailure(JobId jobId, TaskAttemptId taskId, String nodeId, String details)
            throws Exception {
        TaskFailureWork tfe = new TaskFailureWork(this, jobId, taskId, nodeId, details);
        workQueue.schedule(tfe);
    }

    @Override
    public JobStatus getJobStatus(JobId jobId) throws Exception {
        GetJobStatusWork gse = new GetJobStatusWork(this, jobId);
        workQueue.scheduleAndSync(gse);
        return gse.getStatus();
    }

    @Override
    public void start(JobId jobId) throws Exception {
        JobStartWork jse = new JobStartWork(this, jobId);
        workQueue.schedule(jse);
    }

    @Override
    public void waitForCompletion(JobId jobId) throws Exception {
        GetJobStatusConditionVariableWork e = new GetJobStatusConditionVariableWork(this, jobId);
        workQueue.scheduleAndSync(e);
        IJobStatusConditionVariable var = e.getConditionVariable();
        if (var != null) {
            var.waitForCompletion();
        }
    }

    @Override
    public void reportProfile(String id, List<JobProfile> profiles) throws Exception {
        workQueue.schedule(new ReportProfilesWork(this, profiles));
    }

    @Override
    public synchronized void nodeHeartbeat(String id, HeartbeatData hbData) throws Exception {
        workQueue.schedule(new NodeHeartbeatWork(this, id, hbData));
    }

    @Override
    public void createApplication(String appName) throws Exception {
        synchronized (applications) {
            if (applications.containsKey(appName)) {
                throw new HyracksException("Duplicate application with name: " + appName + " being created.");
            }
            CCApplicationContext appCtx = new CCApplicationContext(serverCtx, ccContext, appName);
            applications.put(appName, appCtx);
        }
    }

    @Override
    public void destroyApplication(String appName) throws Exception {
        FutureValue fv = new FutureValue();
        workQueue.schedule(new ApplicationDestroyWork(this, appName, fv));
        fv.get();
    }

    @Override
    public void startApplication(final String appName) throws Exception {
        FutureValue fv = new FutureValue();
        workQueue.schedule(new ApplicationStartWork(this, appName, fv));
        fv.get();
    }

    @Override
    public ClusterControllerInfo getClusterControllerInfo() throws Exception {
        return info;
    }

    @Override
    public void registerPartitionProvider(PartitionDescriptor partitionDescriptor) {
        workQueue.schedule(new RegisterPartitionAvailibilityWork(this, partitionDescriptor));
    }

    @Override
    public void registerPartitionRequest(PartitionRequest partitionRequest) {
        workQueue.schedule(new RegisterPartitionRequestWork(this, partitionRequest));
    }

    private class DeadNodeSweeper extends TimerTask {
        @Override
        public void run() {
            workQueue.schedule(new RemoveDeadNodesWork(ClusterControllerService.this));
        }
    }
}