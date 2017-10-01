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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.api.job.resource.IJobCapacityController;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.NodeControllerState;
import org.apache.hyracks.control.cc.application.CCServiceContext;
import org.apache.hyracks.control.cc.cluster.INodeManager;
import org.apache.hyracks.control.cc.cluster.NodeManager;
import org.apache.hyracks.control.common.controllers.CCConfig;
import org.apache.hyracks.control.common.ipc.NodeControllerRemoteProxy;
import org.apache.hyracks.control.common.logs.LogFile;
import org.apache.hyracks.control.common.work.NoOpCallback;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.kohsuke.args4j.CmdLineException;
import org.mockito.Mockito;

public class JobManagerTest {

    private CCConfig ccConfig;

    @Before
    public void setup() throws IOException, CmdLineException {
        ccConfig = new CCConfig();
        ccConfig.getConfigManager().processConfig();
    }

    @Test
    public void test() throws IOException, CmdLineException {
        IJobCapacityController jobCapacityController = mock(IJobCapacityController.class);
        IJobManager jobManager = spy(new JobManager(ccConfig, mockClusterControllerService(), jobCapacityController));

        // Submits runnable jobs.
        List<JobRun> acceptedRuns = new ArrayList<>();
        for (int id = 0; id < 4096; ++id) {
            // Mocks an immediately executable job.
            JobRun run = mockJobRun(id);
            JobSpecification job = mock(JobSpecification.class);
            when(run.getJobSpecification()).thenReturn(job);
            when(jobCapacityController.allocate(job)).thenReturn(IJobCapacityController.JobSubmissionStatus.EXECUTE);

            // Submits the job.
            acceptedRuns.add(run);
            jobManager.add(run);
            Assert.assertTrue(jobManager.getRunningJobs().size() == id + 1);
            Assert.assertTrue(jobManager.getPendingJobs().isEmpty());
        }

        // Submits jobs that will be deferred due to the capacity limitation.
        List<JobRun> deferredRuns = new ArrayList<>();
        for (int id = 4096; id < 8192; ++id) {
            // Mocks a deferred job.
            JobRun run = mockJobRun(id);
            JobSpecification job = mock(JobSpecification.class);
            when(run.getJobSpecification()).thenReturn(job);
            when(jobCapacityController.allocate(job)).thenReturn(IJobCapacityController.JobSubmissionStatus.QUEUE)
                    .thenReturn(IJobCapacityController.JobSubmissionStatus.EXECUTE);

            // Submits the job.
            deferredRuns.add(run);
            jobManager.add(run);
            Assert.assertTrue(jobManager.getRunningJobs().size() == 4096);
            Assert.assertTrue(jobManager.getPendingJobs().size() == id + 1 - 4096);
        }

        // Further jobs will be denied because the job queue is full.
        boolean jobQueueFull = false;
        try {
            JobRun run = mockJobRun(8193);
            JobSpecification job = mock(JobSpecification.class);
            when(run.getJobSpecification()).thenReturn(job);
            when(jobCapacityController.allocate(job)).thenReturn(IJobCapacityController.JobSubmissionStatus.QUEUE)
                    .thenReturn(IJobCapacityController.JobSubmissionStatus.EXECUTE);
            jobManager.add(run);
        } catch (HyracksException e) {
            // Verifies the error code.
            jobQueueFull = e.getErrorCode() == ErrorCode.JOB_QUEUE_FULL;
        }
        Assert.assertTrue(jobQueueFull);

        // Completes runnable jobs.
        for (JobRun run : acceptedRuns) {
            jobManager.prepareComplete(run, JobStatus.TERMINATED, Collections.emptyList());
            jobManager.finalComplete(run);
        }
        Assert.assertTrue(jobManager.getRunningJobs().size() == 4096);
        Assert.assertTrue(jobManager.getPendingJobs().isEmpty());
        Assert.assertTrue(jobManager.getArchivedJobs().size() == ccConfig.getJobHistorySize());

        // Completes deferred jobs.
        for (JobRun run : deferredRuns) {
            jobManager.prepareComplete(run, JobStatus.TERMINATED, Collections.emptyList());
            jobManager.finalComplete(run);
        }
        Assert.assertTrue(jobManager.getRunningJobs().isEmpty());
        Assert.assertTrue(jobManager.getPendingJobs().isEmpty());
        Assert.assertTrue(jobManager.getArchivedJobs().size() == ccConfig.getJobHistorySize());
        verify(jobManager, times(8192)).prepareComplete(any(), any(), any());
        verify(jobManager, times(8192)).finalComplete(any());
    }

    @Test
    public void testExceedMax() throws HyracksException {
        IJobCapacityController jobCapacityController = mock(IJobCapacityController.class);
        IJobManager jobManager = spy(new JobManager(ccConfig, mockClusterControllerService(), jobCapacityController));
        boolean rejected = false;
        // A job should be rejected immediately if its requirement exceeds the maximum capacity of the cluster.
        try {
            JobRun run = mockJobRun(1);
            JobSpecification job = mock(JobSpecification.class);
            when(run.getJobSpecification()).thenReturn(job);
            when(jobCapacityController.allocate(job))
                    .thenThrow(HyracksException.create(ErrorCode.JOB_REQUIREMENTS_EXCEED_CAPACITY, "1", "0"));
            jobManager.add(run);
        } catch (HyracksException e) {
            // Verifies the error code.
            rejected = e.getErrorCode() == ErrorCode.JOB_REQUIREMENTS_EXCEED_CAPACITY;
        }
        Assert.assertTrue(rejected);
        Assert.assertTrue(jobManager.getRunningJobs().isEmpty());
        Assert.assertTrue(jobManager.getPendingJobs().isEmpty());
        Assert.assertTrue(jobManager.getArchivedJobs().size() == 0);
    }

    @Test
    public void testAdmitThenReject() throws HyracksException {
        IJobCapacityController jobCapacityController = mock(IJobCapacityController.class);
        IJobManager jobManager = spy(new JobManager(ccConfig, mockClusterControllerService(), jobCapacityController));

        // A pending job should also be rejected if its requirement exceeds the updated maximum capacity of the cluster.
        // A normal run.
        JobRun run1 = mockJobRun(1);
        JobSpecification job1 = mock(JobSpecification.class);
        when(run1.getJobSpecification()).thenReturn(job1);
        when(jobCapacityController.allocate(job1)).thenReturn(IJobCapacityController.JobSubmissionStatus.EXECUTE);
        jobManager.add(run1);

        // A failure run.
        JobRun run2 = mockJobRun(2);
        JobSpecification job2 = mock(JobSpecification.class);
        when(run2.getJobSpecification()).thenReturn(job2);
        when(jobCapacityController.allocate(job2)).thenReturn(IJobCapacityController.JobSubmissionStatus.QUEUE)
                .thenThrow(HyracksException.create(ErrorCode.JOB_REQUIREMENTS_EXCEED_CAPACITY, "1", "0"));
        jobManager.add(run2);

        // Completes the first run.
        jobManager.prepareComplete(run1, JobStatus.TERMINATED, Collections.emptyList());
        jobManager.finalComplete(run1);

        // Verifies job status of the failed job.
        verify(run2, times(1)).setStatus(eq(JobStatus.PENDING), any());
        verify(run2, times(1)).setPendingStatus(eq(JobStatus.FAILURE), any());
    }

    @Test
    public void testNullJob() throws HyracksException {
        IJobCapacityController jobCapacityController = mock(IJobCapacityController.class);
        IJobManager jobManager = new JobManager(ccConfig, mockClusterControllerService(), jobCapacityController);
        boolean invalidParameter = false;
        try {
            jobManager.add(null);
        } catch (HyracksException e) {
            invalidParameter = e.getErrorCode() == ErrorCode.INVALID_INPUT_PARAMETER;
        }
        Assert.assertTrue(invalidParameter);
        Assert.assertTrue(jobManager.getRunningJobs().isEmpty());
        Assert.assertTrue(jobManager.getPendingJobs().isEmpty());
    }

    @Test
    public void testCancel() throws Exception {
        CCConfig ccConfig = new CCConfig();
        IJobCapacityController jobCapacityController = mock(IJobCapacityController.class);
        IJobManager jobManager = spy(new JobManager(ccConfig, mockClusterControllerService(), jobCapacityController));

        // Submits runnable jobs.
        List<JobRun> acceptedRuns = new ArrayList<>();
        for (int id = 0; id < 4096; ++id) {
            // Mocks an immediately executable job.
            JobRun run = mockJobRun(id);
            JobSpecification job = mock(JobSpecification.class);
            when(run.getJobSpecification()).thenReturn(job);
            when(jobCapacityController.allocate(job)).thenReturn(IJobCapacityController.JobSubmissionStatus.EXECUTE);

            // Submits the job.
            acceptedRuns.add(run);
            jobManager.add(run);
            Assert.assertTrue(jobManager.getRunningJobs().size() == id + 1);
            Assert.assertTrue(jobManager.getPendingJobs().isEmpty());
        }

        // Submits jobs that will be deferred due to the capacity limitation.
        List<JobRun> deferredRuns = new ArrayList<>();
        for (int id = 4096; id < 8192; ++id) {
            // Mocks a deferred job.
            JobRun run = mockJobRun(id);
            JobSpecification job = mock(JobSpecification.class);
            when(run.getJobSpecification()).thenReturn(job);
            when(jobCapacityController.allocate(job)).thenReturn(IJobCapacityController.JobSubmissionStatus.QUEUE)
                    .thenReturn(IJobCapacityController.JobSubmissionStatus.EXECUTE);

            // Submits the job.
            deferredRuns.add(run);
            jobManager.add(run);
            Assert.assertTrue(jobManager.getRunningJobs().size() == 4096);
            Assert.assertTrue(jobManager.getPendingJobs().size() == id + 1 - 4096);
        }

        // Cancels deferred jobs.
        for (JobRun run : deferredRuns) {
            jobManager.cancel(run.getJobId(), NoOpCallback.INSTANCE);
        }

        // Cancels runnable jobs.
        for (JobRun run : acceptedRuns) {
            jobManager.cancel(run.getJobId(), NoOpCallback.INSTANCE);
        }

        Assert.assertTrue(jobManager.getPendingJobs().isEmpty());
        Assert.assertTrue(jobManager.getArchivedJobs().size() == ccConfig.getJobHistorySize());
        verify(jobManager, times(0)).prepareComplete(any(), any(), any());
        verify(jobManager, times(0)).finalComplete(any());
    }

    private JobRun mockJobRun(long id) {
        JobRun run = mock(JobRun.class, Mockito.RETURNS_DEEP_STUBS);
        when(run.getExceptions()).thenReturn(Collections.emptyList());
        when(run.getActivityClusterGraph().isReportTaskDetails()).thenReturn(true);
        when(run.getPendingExceptions()).thenReturn(Collections.emptyList());
        JobId jobId = new JobId(id);
        when(run.getJobId()).thenReturn(jobId);

        Set<String> nodes = new HashSet<>();
        nodes.add("node1");
        nodes.add("node2");
        when(run.getParticipatingNodeIds()).thenReturn(nodes);
        when(run.getCleanupPendingNodeIds()).thenReturn(nodes);
        return run;
    }

    private ClusterControllerService mockClusterControllerService() {
        ClusterControllerService ccs = mock(ClusterControllerService.class);
        CCServiceContext ccServiceCtx = mock(CCServiceContext.class);
        LogFile logFile = mock(LogFile.class);
        INodeManager nodeManager = mockNodeManager();
        when(ccs.getContext()).thenReturn(ccServiceCtx);
        when(ccs.getJobLogFile()).thenReturn(logFile);
        when(ccs.getNodeManager()).thenReturn(nodeManager);
        when(ccs.getCCConfig()).thenReturn(ccConfig);
        return ccs;
    }

    private INodeManager mockNodeManager() {
        INodeManager nodeManager = mock(NodeManager.class);
        NodeControllerState ncState = mock(NodeControllerState.class);
        NodeControllerRemoteProxy nodeController = mock(NodeControllerRemoteProxy.class);
        when(nodeManager.getNodeControllerState(any())).thenReturn(ncState);
        when(ncState.getNodeController()).thenReturn(nodeController);
        return nodeManager;
    }

}
