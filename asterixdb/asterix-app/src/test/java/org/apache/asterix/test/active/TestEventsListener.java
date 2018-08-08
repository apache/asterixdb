/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.test.active;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.active.ActivityState;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.IRetryPolicyFactory;
import org.apache.asterix.app.active.ActiveEntityEventsListener;
import org.apache.asterix.common.api.IMetadataLockManager;
import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.common.metadata.LockList;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobIdFactory;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.util.ExitUtil;

public class TestEventsListener extends ActiveEntityEventsListener {

    public static enum Behavior {
        SUCCEED,
        RUNNING_JOB_FAIL,
        FAIL_COMPILE,
        FAIL_RUNTIME,
        FAIL_START_TIMEOUT_OP_SUCCEED,
        FAIL_START_TIMEOUT_STUCK,
        FAIL_STOP_TIMEOUT,
        STEP_SUCCEED,
        STEP_FAIL_COMPILE,
        STEP_FAIL_RUNTIME
    }

    private final Semaphore step = new Semaphore(0);
    private final TestClusterControllerActor clusterController;
    private final TestNodeControllerActor[] nodeControllers;
    private final JobIdFactory jobIdFactory;
    private Behavior onStart = Behavior.FAIL_COMPILE;
    private Behavior onStop = Behavior.FAIL_COMPILE;

    public TestEventsListener(TestClusterControllerActor clusterController, TestNodeControllerActor[] nodeControllers,
            JobIdFactory jobIdFactory, EntityId entityId, List<Dataset> datasets, IStatementExecutor statementExecutor,
            ICcApplicationContext appCtx, IHyracksClientConnection hcc, AlgebricksAbsolutePartitionConstraint locations,
            IRetryPolicyFactory retryPolicyFactory) throws HyracksDataException {
        super(statementExecutor, appCtx, hcc, entityId, datasets, locations, TestEventsListener.class.getSimpleName(),
                retryPolicyFactory);
        this.clusterController = clusterController;
        this.nodeControllers = nodeControllers;
        this.jobIdFactory = jobIdFactory;
    }

    public void allowStep() {
        step.release();
    }

    private void step(Behavior behavior) throws HyracksDataException {
        if (behavior == Behavior.STEP_FAIL_COMPILE || behavior == Behavior.STEP_FAIL_RUNTIME
                || behavior == Behavior.STEP_SUCCEED) {
            takeStep();
        }
    }

    @SuppressWarnings("deprecation")
    private void failCompile(Behavior behavior) throws HyracksDataException {
        if (behavior == Behavior.FAIL_COMPILE || behavior == Behavior.STEP_FAIL_COMPILE) {
            throw new HyracksDataException("Compilation Failure");
        }
    }

    private synchronized void takeStep() throws HyracksDataException {
        try {
            while (!step.tryAcquire()) {
                notifyAll();
                wait(10);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw HyracksDataException.create(e);
        }
    }

    @Override
    protected JobId compileAndStartJob(MetadataProvider metadataProvider) throws HyracksDataException {
        step(onStart);
        try {
            metadataProvider.getApplicationContext().getMetadataLockManager()
                    .acquireDatasetReadLock(metadataProvider.getLocks(), "Default.type");
        } catch (AlgebricksException e) {
            throw HyracksDataException.create(e);
        }
        failCompile(onStart);
        JobId jobId = jobIdFactory.create();
        Action startJob = clusterController.startActiveJob(jobId, entityId);
        try {
            startJob.sync();
        } catch (InterruptedException e) {
            ExitUtil.halt(ExitUtil.EC_ABNORMAL_TERMINATION);
        }
        if (onStart == Behavior.FAIL_RUNTIME || onStart == Behavior.STEP_FAIL_RUNTIME) {
            clusterController.jobFinish(jobId, JobStatus.FAILURE,
                    Collections.singletonList(new HyracksDataException("RuntimeFailure")));
        } else if (onStart != Behavior.FAIL_START_TIMEOUT_OP_SUCCEED && onStart != Behavior.FAIL_START_TIMEOUT_STUCK) {
            for (int i = 0; i < nodeControllers.length; i++) {
                TestNodeControllerActor nodeController = nodeControllers[i];
                nodeController.registerRuntime(jobId, entityId, i);
            }
        }
        if (onStart == Behavior.FAIL_START_TIMEOUT_OP_SUCCEED) {
            for (int i = 0; i < nodeControllers.length; i++) {
                TestNodeControllerActor nodeController = nodeControllers[i];
                try {
                    nodeController.registerRuntime(jobId, entityId, i).sync();
                } catch (InterruptedException e) {
                    ExitUtil.halt(ExitUtil.EC_ABNORMAL_TERMINATION);
                }
            }
            // At this point, the job has started and both nodes reported that they started.
            // but since we're holding the lock on the listener (this is a synchronized method), the state
            // didn't change yet
            while (state != ActivityState.RUNNING) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    ExitUtil.halt(ExitUtil.EC_ABNORMAL_TERMINATION);
                }
            }
            Thread.currentThread().interrupt();
        } else if (onStart == Behavior.FAIL_START_TIMEOUT_STUCK) {
            TestNodeControllerActor nodeController = nodeControllers[0];
            try {
                nodeController.registerRuntime(jobId, entityId, 0).sync();
            } catch (InterruptedException e) {
                ExitUtil.halt(ExitUtil.EC_ABNORMAL_TERMINATION);
            }
            Thread.currentThread().interrupt();
        }
        return jobId;
    }

    @Override
    protected void cancelJobSafely(MetadataProvider metadataProvider, Throwable th) {
        clusterController.jobFinish(jobId, JobStatus.FAILURE,
                Collections.singletonList(HyracksException.create(ErrorCode.JOB_CANCELED, jobId)));
    }

    @Override
    protected void sendStopMessages(MetadataProvider metadataProvider, long timeout, TimeUnit unit) throws Exception {
        step(onStop);
        failCompile(onStop);
        if (onStop == Behavior.RUNNING_JOB_FAIL) {
            clusterController.jobFinish(jobId, JobStatus.FAILURE,
                    Collections.singletonList(new HyracksDataException("RuntimeFailure")));
        } else if (onStop == Behavior.FAIL_STOP_TIMEOUT) {
            // Nothing happens.
            Thread.currentThread().interrupt();
        } else {
            for (int i = 0; i < nodeControllers.length; i++) {
                TestNodeControllerActor nodeController = nodeControllers[0];
                nodeController.deRegisterRuntime(jobId, entityId, i).sync();
            }
            clusterController.jobFinish(jobId, JobStatus.TERMINATED, Collections.emptyList());
        }
    }

    public void onStart(Behavior behavior) {
        this.onStart = behavior;
    }

    public void onStop(Behavior behavior) {
        if (behavior == Behavior.FAIL_COMPILE) {
            throw new IllegalArgumentException("Test framework is not designed for this case");
        }
        this.onStop = behavior;
    }

    @Override
    protected void setRunning(MetadataProvider metadataProvider, boolean running) {
        try {
            IMetadataLockManager lockManager = metadataProvider.getApplicationContext().getMetadataLockManager();
            LockList locks = metadataProvider.getLocks();
            lockManager.acquireDataverseReadLock(locks, entityId.getDataverse());
            lockManager.acquireActiveEntityWriteLock(locks, entityId.getDataverse() + '.' + entityId.getEntityName());
            // persist entity
        } catch (Throwable th) {
            // This failure puts the system in a bad state.
            throw new IllegalStateException(th);
        }
    }

    @Override
    protected void doSuspend(MetadataProvider metadataProvider) throws HyracksDataException {
        doStop(metadataProvider, appCtx.getActiveProperties().getActiveSuspendTimeout(), TIMEOUT_UNIT);
    }

    @Override
    protected void doResume(MetadataProvider metadataProvider) throws HyracksDataException {
        doStart(metadataProvider);
    }

    @Override
    protected ActiveRuntimeId getActiveRuntimeId(int partition) {
        throw new UnsupportedOperationException();
    }
}
