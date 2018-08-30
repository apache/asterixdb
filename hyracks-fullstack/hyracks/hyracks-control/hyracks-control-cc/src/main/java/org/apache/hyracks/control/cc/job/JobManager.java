/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hyracks.control.cc.job;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.ActivityClusterGraph;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.job.resource.IJobCapacityController;
import org.apache.hyracks.api.util.ExceptionUtils;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.NodeControllerState;
import org.apache.hyracks.control.cc.application.CCServiceContext;
import org.apache.hyracks.control.cc.cluster.INodeManager;
import org.apache.hyracks.control.cc.scheduler.FIFOJobQueue;
import org.apache.hyracks.control.cc.scheduler.IJobQueue;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.work.IResultCallback;
import org.apache.hyracks.control.common.work.NoOpCallback;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

// Job manager manages all jobs that haven been submitted to the cluster.
public class JobManager implements IJobManager {

    private static final Logger LOGGER = LogManager.getLogger();

    private final ClusterControllerService ccs;
    private final Map<JobId, JobRun> activeRunMap;
    private final Map<JobId, JobRun> runMapArchive;
    private final Map<JobId, List<Exception>> runMapHistory;
    private final IJobCapacityController jobCapacityController;
    private IJobQueue jobQueue;

    public JobManager(CCConfig ccConfig, ClusterControllerService ccs, IJobCapacityController jobCapacityController) {
        this.ccs = ccs;
        this.jobCapacityController = jobCapacityController;
        try {
            Constructor<?> jobQueueConstructor = this.getClass().getClassLoader().loadClass(ccConfig.getJobQueueClass())
                    .getConstructor(IJobManager.class, IJobCapacityController.class);
            jobQueue = (IJobQueue) jobQueueConstructor.newInstance(this, this.jobCapacityController);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException
                | InvocationTargetException e) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.log(Level.WARN, "class " + ccConfig.getJobQueueClass() + " could not be used: ", e);
            }
            // Falls back to the default implementation if the user-provided class name is not valid.
            jobQueue = new FIFOJobQueue(this, jobCapacityController);
        }
        activeRunMap = new HashMap<>();
        runMapArchive = new LinkedHashMap<JobId, JobRun>() {
            private static final long serialVersionUID = 1L;

            @Override
            protected boolean removeEldestEntry(Map.Entry<JobId, JobRun> eldest) {
                return size() > ccConfig.getJobHistorySize();
            }
        };
        runMapHistory = new LinkedHashMap<JobId, List<Exception>>() {
            private static final long serialVersionUID = 1L;
            /** history size + 1 is for the case when history size = 0 */
            private final int allowedSize = 100 * (ccConfig.getJobHistorySize() + 1);

            @Override
            protected boolean removeEldestEntry(Map.Entry<JobId, List<Exception>> eldest) {
                return size() > allowedSize;
            }
        };
    }

    @Override
    public void add(JobRun jobRun) throws HyracksException {
        checkJob(jobRun);
        JobSpecification job = jobRun.getJobSpecification();
        IJobCapacityController.JobSubmissionStatus status = jobCapacityController.allocate(job);
        CCServiceContext serviceCtx = ccs.getContext();
        serviceCtx.notifyJobCreation(jobRun.getJobId(), job);
        switch (status) {
            case QUEUE:
                queueJob(jobRun);
                break;
            case EXECUTE:
                executeJob(jobRun);
                break;
            default:
                throw new IllegalStateException("unknown submission status: " + status);
        }
    }

    @Override
    public void cancel(JobId jobId, IResultCallback<Void> callback) throws HyracksException {
        // Cancels a running job.
        if (activeRunMap.containsKey(jobId)) {
            JobRun jobRun = activeRunMap.get(jobId);
            // The following call will abort all ongoing tasks and then consequently
            // trigger JobCleanupWork and JobCleanupNotificationWork which will update the lifecyle of the job.
            // Therefore, we do not remove the job out of activeRunMap here.
            jobRun.getExecutor().cancelJob(callback);
            return;
        }
        // Removes a pending job.
        JobRun jobRun = jobQueue.remove(jobId);
        if (jobRun != null) {
            List<Exception> exceptions =
                    Collections.singletonList(HyracksException.create(ErrorCode.JOB_CANCELED, jobId));
            // Since the job has not been executed, we only need to update its status and lifecyle here.
            jobRun.setStatus(JobStatus.FAILURE_BEFORE_EXECUTION, exceptions);
            runMapArchive.put(jobId, jobRun);
            runMapHistory.put(jobId, exceptions);
            CCServiceContext serviceCtx = ccs.getContext();
            if (serviceCtx != null) {
                try {
                    serviceCtx.notifyJobFinish(jobId, JobStatus.FAILURE_BEFORE_EXECUTION, exceptions);
                } catch (Exception e) {
                    LOGGER.error("Exception notifying cancel on pending job {}", jobId, e);
                    throw HyracksDataException.create(e);
                }
            }
        }
        callback.setValue(null);
    }

    @Override
    public void prepareComplete(JobRun run, JobStatus status, List<Exception> exceptions) throws HyracksException {
        checkJob(run);
        ccs.removeJobParameterByteStore(run.getJobId());
        if (status == JobStatus.FAILURE_BEFORE_EXECUTION) {
            run.setPendingStatus(JobStatus.FAILURE, exceptions);
            finalComplete(run);
            return;
        }
        if (run.getPendingStatus() != null && run.getCleanupPendingNodeIds().isEmpty()) {
            finalComplete(run);
            return;
        }
        if (run.getPendingStatus() != null) {
            LOGGER.warn("Ignoring duplicate cleanup for JobRun with id: {}", run::getJobId);
            return;
        }
        Set<String> targetNodes = run.getParticipatingNodeIds();
        run.getCleanupPendingNodeIds().addAll(targetNodes);
        if (run.getPendingStatus() != JobStatus.FAILURE && run.getPendingStatus() != JobStatus.TERMINATED) {
            run.setPendingStatus(status, exceptions);
        }

        if (!targetNodes.isEmpty()) {
            cleanupJobOnNodes(run, status, targetNodes);
        } else {
            finalComplete(run);
        }

    }

    private void cleanupJobOnNodes(JobRun run, JobStatus status, Set<String> targetNodes) throws HyracksException {
        Throwable caughtException = null;
        JobId jobId = run.getJobId();
        INodeManager nodeManager = ccs.getNodeManager();
        Set<String> toDelete = new HashSet<>();
        for (String n : targetNodes) {
            NodeControllerState ncs = nodeManager.getNodeControllerState(n);
            if (ncs == null) {
                toDelete.add(n);
            } else {
                try {
                    ncs.getNodeController().cleanUpJoblet(jobId, status);
                } catch (Exception e) {
                    LOGGER.error("Exception cleaning up joblet {} on node {}", jobId, n, e);
                    caughtException = ExceptionUtils.suppress(caughtException, e);
                }
            }
        }
        targetNodes.removeAll(toDelete);
        run.getCleanupPendingNodeIds().removeAll(toDelete);
        if (run.getCleanupPendingNodeIds().isEmpty()) {
            finalComplete(run);
        }
        // throws caught exceptions if any
        if (caughtException != null) {
            throw HyracksException.wrapOrThrowUnchecked(caughtException);
        }
    }

    @Override
    public void finalComplete(JobRun run) throws HyracksException {
        checkJob(run);
        JobId jobId = run.getJobId();
        Throwable caughtException = null;
        CCServiceContext serviceCtx = ccs.getContext();
        try {
            serviceCtx.notifyJobFinish(jobId, run.getPendingStatus(), run.getPendingExceptions());
        } catch (Exception e) {
            LOGGER.error("Exception notifying job finish {}", jobId, e);
            caughtException = e;
        }
        run.setStatus(run.getPendingStatus(), run.getPendingExceptions());
        run.setEndTime(System.currentTimeMillis());
        if (activeRunMap.remove(jobId) != null) {
            // non-active jobs have zero capacity
            releaseJobCapacity(run);
        }
        runMapArchive.put(jobId, run);
        runMapHistory.put(jobId, run.getExceptions());

        if (run.getActivityClusterGraph().isReportTaskDetails()) {
            /*
             * log job details when profiling is enabled
             */
            try {
                ccs.getJobLogFile().log(createJobLogObject(run));
            } catch (Exception e) {
                LOGGER.error("Exception reporting task details for job {}", jobId, e);
                caughtException = ExceptionUtils.suppress(caughtException, e);
            }
        }

        // Picks the next job to execute.
        pickJobsToRun();

        // throws caught exceptions if any
        if (caughtException != null) {
            throw HyracksException.wrapOrThrowUnchecked(caughtException);
        }
    }

    @Override
    public Collection<JobRun> getRunningJobs() {
        return activeRunMap.values();
    }

    @Override
    public Collection<JobRun> getPendingJobs() {
        return jobQueue.jobs();
    }

    @Override
    public Collection<JobRun> getArchivedJobs() {
        return runMapArchive.values();
    }

    @Override
    public JobRun get(JobId jobId) {
        JobRun jobRun = activeRunMap.get(jobId); // Running job.
        if (jobRun == null) {
            jobRun = jobQueue.get(jobId); // Pending job.
        }
        if (jobRun == null) {
            jobRun = runMapArchive.get(jobId); // Completed job.
        }
        return jobRun;
    }

    @Override
    public List<Exception> getExceptionHistory(JobId jobId) {
        List<Exception> exceptions = runMapHistory.get(jobId);
        return exceptions == null ? runMapHistory.containsKey(jobId) ? Collections.emptyList() : null : exceptions;
    }

    @Override
    public int getJobQueueCapacity() {
        return ccs.getCCConfig().getJobQueueCapacity();
    }

    private void pickJobsToRun() throws HyracksException {
        List<JobRun> selectedRuns = jobQueue.pull();
        for (JobRun run : selectedRuns) {
            executeJob(run);
        }
    }

    // Executes a job when the required capacity for the job is met.
    private void executeJob(JobRun run) throws HyracksException {
        run.setStartTime(System.currentTimeMillis());
        JobId jobId = run.getJobId();
        activeRunMap.put(jobId, run);
        run.setStatus(JobStatus.RUNNING, null);
        executeJobInternal(run);
    }

    // Queue a job when the required capacity for the job is not met.
    private void queueJob(JobRun jobRun) throws HyracksException {
        jobRun.setStatus(JobStatus.PENDING, null);
        jobQueue.add(jobRun);
    }

    private void executeJobInternal(JobRun run) {
        try {
            run.getExecutor().startJob();
        } catch (Exception e) {
            LOGGER.log(Level.ERROR, "Aborting " + run.getJobId() + " due to failure during job start", e);
            final List<Exception> exceptions = Collections.singletonList(e);
            // fail the job then abort it
            run.setStatus(JobStatus.FAILURE, exceptions);
            // abort job will trigger JobCleanupWork
            run.getExecutor().abortJob(exceptions, NoOpCallback.INSTANCE);
        }
    }

    private ObjectNode createJobLogObject(final JobRun run) {
        ObjectMapper om = new ObjectMapper();
        ObjectNode jobLogObject = om.createObjectNode();
        ActivityClusterGraph acg = run.getActivityClusterGraph();
        jobLogObject.set("activity-cluster-graph", acg.toJSON());
        jobLogObject.set("job-run", run.toJSON());
        return jobLogObject;
    }

    private void checkJob(JobRun jobRun) throws HyracksException {
        if (jobRun == null) {
            throw HyracksException.create(ErrorCode.INVALID_INPUT_PARAMETER);
        }
    }

    private void releaseJobCapacity(JobRun jobRun) {
        final JobSpecification job = jobRun.getJobSpecification();
        jobCapacityController.release(job);
    }
}
