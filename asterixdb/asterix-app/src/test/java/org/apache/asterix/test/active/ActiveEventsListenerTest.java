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

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.asterix.active.ActivityState;
import org.apache.asterix.active.CountRetryPolicyFactory;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.InfiniteRetryPolicyFactory;
import org.apache.asterix.active.NoRetryPolicyFactory;
import org.apache.asterix.app.active.ActiveNotificationHandler;
import org.apache.asterix.common.api.IClusterManagementWork.ClusterState;
import org.apache.asterix.common.api.IMetadataLockManager;
import org.apache.asterix.common.cluster.IClusterStateManager;
import org.apache.asterix.common.config.ActiveProperties;
import org.apache.asterix.common.context.IStorageComponentProvider;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.feed.watch.WaitForStateSubscriber;
import org.apache.asterix.file.StorageComponentProvider;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.metadata.entities.Feed;
import org.apache.asterix.metadata.lock.MetadataLockManager;
import org.apache.asterix.om.functions.IFunctionExtensionManager;
import org.apache.asterix.runtime.functions.FunctionCollection;
import org.apache.asterix.runtime.functions.FunctionManager;
import org.apache.asterix.runtime.utils.CcApplicationContext;
import org.apache.asterix.test.active.TestEventsListener.Behavior;
import org.apache.asterix.test.base.TestMethodTracer;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.control.CcId;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobIdFactory;
import org.apache.hyracks.api.job.JobStatus;
import org.apache.hyracks.control.cc.ClusterControllerService;
import org.apache.hyracks.control.cc.application.CCServiceContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.mockito.Mockito;

public class ActiveEventsListenerTest {

    static TestClusterControllerActor clusterController;
    static TestNodeControllerActor[] nodeControllers;
    static TestUserActor[] users;
    static String[] nodes = { "node1", "node2" };
    static ActiveNotificationHandler handler;
    static String dataverseName = "Default";
    static String entityName = "entityName";
    static EntityId entityId = new EntityId(Feed.EXTENSION_NAME, dataverseName, entityName);
    static Dataset firstDataset;
    static Dataset secondDataset;
    static List<Dataset> allDatasets;
    static TestEventsListener listener;
    static IClusterStateManager clusterStateManager;
    static CcApplicationContext appCtx;
    static IStatementExecutor statementExecutor;
    static IHyracksClientConnection hcc;
    static IFunctionExtensionManager functionExtensionManager;
    static MetadataProvider metadataProvider;
    static IStorageComponentProvider componentProvider;
    static JobIdFactory jobIdFactory;
    static IMetadataLockManager lockManager = new MetadataLockManager();
    static AlgebricksAbsolutePartitionConstraint locations;
    static ExecutorService executor;

    @Rule
    public TestRule watcher = new TestMethodTracer();

    @Before
    public void setUp() throws Exception {
        jobIdFactory = new JobIdFactory(CcId.valueOf((short) 0));
        handler = new ActiveNotificationHandler();
        allDatasets = new ArrayList<>();
        firstDataset = new Dataset(dataverseName, "firstDataset", null, null, null, null, null, null, null, null, 0, 0);
        secondDataset =
                new Dataset(dataverseName, "secondDataset", null, null, null, null, null, null, null, null, 0, 0);
        allDatasets.add(firstDataset);
        allDatasets.add(secondDataset);
        AtomicInteger threadCounter = new AtomicInteger(0);
        executor = Executors.newCachedThreadPool(
                r -> new Thread(r, "ClusterControllerServiceExecutor[" + threadCounter.getAndIncrement() + "]"));
        clusterStateManager = Mockito.mock(IClusterStateManager.class);
        Mockito.when(clusterStateManager.getState()).thenReturn(ClusterState.ACTIVE);
        ClusterControllerService ccService = Mockito.mock(ClusterControllerService.class);
        CCServiceContext ccServiceCtx = Mockito.mock(CCServiceContext.class);
        appCtx = Mockito.mock(CcApplicationContext.class);
        statementExecutor = Mockito.mock(IStatementExecutor.class);
        hcc = Mockito.mock(IHyracksClientConnection.class);
        Mockito.when(appCtx.getActiveNotificationHandler()).thenReturn(handler);
        Mockito.when(appCtx.getMetadataLockManager()).thenReturn(lockManager);
        Mockito.when(appCtx.getServiceContext()).thenReturn(ccServiceCtx);
        Mockito.when(appCtx.getClusterStateManager()).thenReturn(clusterStateManager);
        Mockito.when(appCtx.getActiveProperties()).thenReturn(Mockito.mock(ActiveProperties.class));
        componentProvider = new StorageComponentProvider();
        Mockito.when(appCtx.getStorageComponentProvider()).thenReturn(componentProvider);
        Mockito.when(ccServiceCtx.getControllerService()).thenReturn(ccService);
        Mockito.when(ccService.getExecutor()).thenReturn(executor);
        locations = new AlgebricksAbsolutePartitionConstraint(nodes);
        functionExtensionManager = Mockito.mock(IFunctionExtensionManager.class);
        Mockito.when(functionExtensionManager.getFunctionManager())
                .thenReturn(new FunctionManager(FunctionCollection.createDefaultFunctionCollection()));
        Mockito.when(appCtx.getExtensionManager()).thenReturn(functionExtensionManager);
        metadataProvider = new MetadataProvider(appCtx, null);
        clusterController = new TestClusterControllerActor("CC", handler, allDatasets);
        nodeControllers = new TestNodeControllerActor[2];
        nodeControllers[0] = new TestNodeControllerActor(nodes[0], clusterController);
        nodeControllers[1] = new TestNodeControllerActor(nodes[1], clusterController);
        listener = new TestEventsListener(clusterController, nodeControllers, jobIdFactory, entityId,
                new ArrayList<>(allDatasets), statementExecutor, appCtx, hcc, locations,
                new InfiniteRetryPolicyFactory());
        users = new TestUserActor[3];
        users[0] = newUser("Till", appCtx);
        users[1] = newUser("Mike", appCtx);
        users[2] = newUser("Dmitry", appCtx);
    }

    TestUserActor newUser(String name, CcApplicationContext appCtx) {
        MetadataProvider actorMdProvider = new MetadataProvider(appCtx, null);
        return new TestUserActor("User: " + name, actorMdProvider, clusterController);
    }

    @After
    public void tearDown() throws Exception {
        executor.shutdownNow();
        executor.awaitTermination(5, TimeUnit.SECONDS);
        handler.stop();
        for (Actor user : users) {
            user.stop();
        }
        for (Actor nc : nodeControllers) {
            nc.stop();
        }
        clusterController.stop();
    }

    @Test
    public void testStartWhenStartSucceed() throws Exception {
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
        listener.onStart(Behavior.SUCCEED);
        Action action = users[0].startActivity(listener);
        action.sync();
        assertSuccess(action);
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
    }

    @Test
    public void testStartWhenStartFailsCompile() throws Exception {
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
        listener.onStart(Behavior.FAIL_COMPILE);
        Action action = users[0].startActivity(listener);
        action.sync();
        assertFailure(action, 0);
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
    }

    @Test
    public void testStartWhenStartFailsRuntime() throws Exception {
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
        listener.onStart(Behavior.FAIL_RUNTIME);
        Action action = users[0].startActivity(listener);
        action.sync();
        assertFailure(action, 0);
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
    }

    @Test
    public void testStartWhenStartSucceedButTimesout() throws Exception {
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
        listener.onStart(Behavior.FAIL_START_TIMEOUT_OP_SUCCEED);
        Action action = users[0].startActivity(listener);
        action.sync();
        assertSuccess(action);
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
    }

    @Test
    public void testStartWhenStartStuckTimesout() throws Exception {
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
        listener.onStart(Behavior.FAIL_START_TIMEOUT_STUCK);
        Action action = users[0].startActivity(listener);
        action.sync();
        assertFailure(action, 0);
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
    }

    @Test
    public void testStopWhenStopTimesout() throws Exception {
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
        listener.onStart(Behavior.SUCCEED);
        Action action = users[0].startActivity(listener);
        action.sync();
        assertSuccess(action);
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
        listener.onStop(Behavior.FAIL_STOP_TIMEOUT);
        action = users[0].stopActivity(listener);
        action.sync();
        assertSuccess(action);
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
    }

    @Test
    public void testStartWhenOneNodeFinishesBeforeOtherNodeStarts() throws Exception {
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
        listener.onStart(Behavior.SUCCEED);
        listener.onStop(Behavior.SUCCEED);
        ActionSubscriber fastSubscriber = new ActionSubscriber();
        nodeControllers[0].subscribe(fastSubscriber);
        ActionSubscriber slowSubscriber = new ActionSubscriber();
        slowSubscriber.stop();
        nodeControllers[1].subscribe(slowSubscriber);
        Action startActivityAction = users[0].startActivity(listener);
        RuntimeRegistration registration = (RuntimeRegistration) fastSubscriber.get(0);
        registration.sync();
        registration.deregister();
        Action deregistration = fastSubscriber.get(1);
        deregistration.sync();
        // Node 0 has completed registration and deregistration.. unblock node 1
        slowSubscriber.resume();
        registration = (RuntimeRegistration) slowSubscriber.get(0);
        registration.sync();
        // now that node 1 is unblocked and completed registration, ensure that start has completed
        startActivityAction.sync();
        assertSuccess(startActivityAction);
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
        Action stopAction = users[0].stopActivity(listener);
        stopAction.sync();
        assertSuccess(stopAction);
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
    }

    @Test
    public void testStopWhenStopSucceed() throws Exception {
        testStartWhenStartSucceed();
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
        listener.onStop(Behavior.SUCCEED);
        Action action = users[0].stopActivity(listener);
        action.sync();
        assertSuccess(action);
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
    }

    @Test
    public void testDoubleStopWhenStopSucceed() throws Exception {
        testStartWhenStartSucceed();
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
        listener.onStop(Behavior.SUCCEED);
        Action firstStop = users[0].stopActivity(listener);
        Action secondStop = users[1].stopActivity(listener);
        firstStop.sync();
        secondStop.sync();
        if (firstStop.hasFailed()) {
            assertFailure(firstStop, ErrorCode.ACTIVE_ENTITY_CANNOT_BE_STOPPED);
            assertSuccess(secondStop);
        } else {
            assertSuccess(firstStop);
            assertFailure(secondStop, ErrorCode.ACTIVE_ENTITY_CANNOT_BE_STOPPED);
        }
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
    }

    @Test
    public void testDoubleStartWhenStartSucceed() throws Exception {
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
        listener.onStart(Behavior.SUCCEED);
        Action firstStart = users[0].startActivity(listener);
        Action secondStart = users[1].startActivity(listener);
        firstStart.sync();
        secondStart.sync();
        if (firstStart.hasFailed()) {
            assertFailure(firstStart, ErrorCode.ACTIVE_ENTITY_ALREADY_STARTED);
            assertSuccess(secondStart);
        } else {
            assertSuccess(firstStart);
            assertFailure(secondStart, ErrorCode.ACTIVE_ENTITY_ALREADY_STARTED);
        }
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
    }

    @Test
    public void testStopAfterDoubleStartWhenStartSucceedAndStopSucceed() throws Exception {
        testDoubleStartWhenStartSucceed();
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
        listener.onStop(Behavior.SUCCEED);
        Action action = users[2].stopActivity(listener);
        action.sync();
        assertSuccess(action);
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
    }

    @Test
    public void testSuspendFromStopped() throws Exception {
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
        listener.onStop(Behavior.SUCCEED);
        Action action = users[0].suspendActivity(listener);
        action.sync();
        assertSuccess(action);
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
        action = users[0].resumeActivity(listener);
        action.sync();
        assertSuccess(action);
    }

    @Test
    public void testStartWhileSuspend() throws Exception {
        listener.onStart(Behavior.SUCCEED);
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
        listener.onStop(Behavior.SUCCEED);
        Action action = users[0].suspendActivity(listener);
        action.sync();
        assertSuccess(action);
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
        // user[0] has the locks
        Action startAction = users[1].startActivity(listener);
        for (int i = 0; i < 100; i++) {
            Assert.assertFalse(startAction.isDone());
        }
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
        Action resumeAction = users[0].resumeActivity(listener);
        resumeAction.sync();
        startAction.sync();
        assertSuccess(resumeAction);
        assertSuccess(startAction);
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
    }

    @Test
    public void testSuspendFromRunning() throws Exception {
        testStartWhenStartSucceed();
        // suspend
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
        listener.onStop(Behavior.SUCCEED);
        Action suspension = users[1].suspendActivity(listener);
        suspension.sync();
        assertSuccess(suspension);
        // resume
        Assert.assertEquals(ActivityState.SUSPENDED, listener.getState());
        Action resumption = users[1].resumeActivity(listener);
        resumption.sync();
        assertSuccess(resumption);
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
    }

    @Test
    public void testSuspendFromRunningAndStopFailThenResumeSucceeds() throws Exception {
        testStartWhenStartSucceed();
        // suspend
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
        listener.onStop(Behavior.RUNNING_JOB_FAIL);
        Action suspension = users[1].suspendActivity(listener);
        suspension.sync();
        Assert.assertFalse(suspension.hasFailed());
        Assert.assertEquals(ActivityState.TEMPORARILY_FAILED, listener.getState());
        Action resumption = users[1].resumeActivity(listener);
        resumption.sync();
        assertSuccess(resumption);
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
    }

    @Test
    public void testStopFromRunningAndJobFails() throws Exception {
        testStartWhenStartSucceed();
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
        listener.onStop(Behavior.STEP_SUCCEED);
        Action stopping = users[1].stopActivity(listener);
        // wait for notification from listener
        synchronized (listener) {
            listener.wait();
        }
        clusterController.jobFinish(listener.getJobId(), JobStatus.FAILURE,
                Collections.singletonList(new HyracksDataException("Runtime Failure")));
        Assert.assertNull(listener.getRecoveryTask());
        listener.allowStep();
        stopping.sync();
        Assert.assertFalse(stopping.hasFailed());
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
        Assert.assertNull(listener.getRecoveryTask());

    }

    @SuppressWarnings("deprecation")
    @Test
    public void testRecovery() throws Exception {
        testStartWhenStartSucceed();
        // suspend
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
        listener.onStart(Behavior.STEP_SUCCEED);
        WaitForStateSubscriber subscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        clusterController.jobFinish(listener.getJobId(), JobStatus.FAILURE,
                Collections.singletonList(new HyracksDataException("Runtime Failure")));
        subscriber.sync();
        Assert.assertNotNull(listener.getRecoveryTask());
        listener.allowStep();
        WaitForStateSubscriber running = new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RUNNING));
        running.sync();
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
        listener.onStop(Behavior.SUCCEED);
        Action stopAction = users[2].stopActivity(listener);
        stopAction.sync();
        assertSuccess(stopAction);
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
    }

    @Test
    public void testSuspendFromRunningButJobFailWhileSuspendingThenResumeSucceed() throws Exception {
        testStartWhenStartSucceed();
        // suspend
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
        listener.onStop(Behavior.RUNNING_JOB_FAIL);
        Action suspension = users[1].suspendActivity(listener);
        suspension.sync();
        assertSuccess(suspension);
        Assert.assertEquals(ActivityState.TEMPORARILY_FAILED, listener.getState());
        Assert.assertNull(listener.getRecoveryTask());
        listener.onStart(Behavior.SUCCEED);
        Action resumption = users[1].resumeActivity(listener);
        resumption.sync();
        assertSuccess(resumption);
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
    }

    @Test
    public void testSuspendFromRunningButJobFailWhileSuspendingThenResumeFailsCompileAndRecoveryStarts()
            throws Exception {
        testStartWhenStartSucceed();
        // suspend
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
        listener.onStop(Behavior.RUNNING_JOB_FAIL);
        Action suspension = users[1].suspendActivity(listener);
        suspension.sync();
        assertSuccess(suspension);
        Assert.assertEquals(ActivityState.TEMPORARILY_FAILED, listener.getState());
        Assert.assertNull(listener.getRecoveryTask());
        listener.onStart(Behavior.FAIL_COMPILE);
        Action resumption = users[1].resumeActivity(listener);
        resumption.sync();
        assertSuccess(resumption);
        ActivityState state = listener.getState();
        Assert.assertTrue(state == ActivityState.RECOVERING || state == ActivityState.TEMPORARILY_FAILED);
        Assert.assertNotNull(listener.getRecoveryTask());
        Action stopActivity = users[1].stopActivity(listener);
        stopActivity.sync();
        assertSuccess(stopActivity);
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
    }

    @Test
    public void testSuspendFromRunningButJobFailWhileSuspendingThenResumeFailsRuntimeAndRecoveryStarts()
            throws Exception {
        testStartWhenStartSucceed();
        // suspend
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
        listener.onStop(Behavior.RUNNING_JOB_FAIL);
        Action suspension = users[1].suspendActivity(listener);
        suspension.sync();
        assertSuccess(suspension);
        Assert.assertEquals(ActivityState.TEMPORARILY_FAILED, listener.getState());
        Assert.assertNull(listener.getRecoveryTask());
        listener.onStart(Behavior.FAIL_RUNTIME);
        Action resumption = users[1].resumeActivity(listener);
        resumption.sync();
        assertSuccess(resumption);
        ActivityState state = listener.getState();
        Assert.assertTrue(state == ActivityState.RECOVERING || state == ActivityState.TEMPORARILY_FAILED);
        Assert.assertNotNull(listener.getRecoveryTask());
        Action stopActivity = users[1].stopActivity(listener);
        stopActivity.sync();
        assertSuccess(stopActivity);
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
    }

    @Test
    public void testStopWhileSuspended() throws Exception {
        testStartWhenStartSucceed();
        // suspend
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
        listener.onStop(Behavior.STEP_SUCCEED);
        Action suspension = users[1].suspendActivity(listener);
        WaitForStateSubscriber subscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.SUSPENDING, ActivityState.SUSPENDED));
        subscriber.sync();
        Action stopping = users[0].stopActivity(listener);
        listener.allowStep();
        listener.allowStep();
        suspension.sync();
        assertSuccess(suspension);
        users[1].resumeActivity(listener);
        stopping.sync();
        assertSuccess(stopping);
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testRecoveryFailureAfterOneAttemptCompilationFailure() throws Exception {
        handler.unregisterListener(listener);
        listener = new TestEventsListener(clusterController, nodeControllers, jobIdFactory, entityId,
                new ArrayList<>(allDatasets), statementExecutor, appCtx, hcc, locations,
                new CountRetryPolicyFactory(1));
        testStartWhenStartSucceed();
        WaitForStateSubscriber tempFailSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        WaitForStateSubscriber permFailSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.STOPPED));
        listener.onStart(Behavior.FAIL_COMPILE);
        clusterController.jobFinish(listener.getJobId(), JobStatus.FAILURE,
                Collections.singletonList(new HyracksDataException("Compilation Failure")));
        tempFailSubscriber.sync();
        permFailSubscriber.sync();
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
    }

    @Test
    public void testStartAfterPermenantFailure() throws Exception {
        testRecoveryFailureAfterOneAttemptCompilationFailure();
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
        listener.onStart(Behavior.SUCCEED);
        WaitForStateSubscriber subscriber = new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RUNNING));
        users[1].startActivity(listener);
        subscriber.sync();
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
    }

    @Test
    public void testStopAfterStartAfterPermenantFailure() throws Exception {
        testStartAfterPermenantFailure();
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
        listener.onStop(Behavior.SUCCEED);
        WaitForStateSubscriber subscriber = new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.STOPPED));
        Action stopAction = users[1].stopActivity(listener);
        subscriber.sync();
        stopAction.sync();
        assertSuccess(stopAction);
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testRecoveryFailureAfterOneAttemptRuntimeFailure() throws Exception {
        handler.unregisterListener(listener);
        listener = new TestEventsListener(clusterController, nodeControllers, jobIdFactory, entityId,
                new ArrayList<>(allDatasets), statementExecutor, appCtx, hcc, locations,
                new CountRetryPolicyFactory(1));
        testStartWhenStartSucceed();
        WaitForStateSubscriber tempFailSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        WaitForStateSubscriber permFailSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.STOPPED));
        listener.onStart(Behavior.FAIL_RUNTIME);
        clusterController.jobFinish(listener.getJobId(), JobStatus.FAILURE,
                Collections.singletonList(new HyracksDataException("Runtime Failure")));
        tempFailSubscriber.sync();
        permFailSubscriber.sync();
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testRecoveryFailure() throws Exception {
        handler.unregisterListener(listener);
        listener = new TestEventsListener(clusterController, nodeControllers, jobIdFactory, entityId,
                new ArrayList<>(allDatasets), statementExecutor, appCtx, hcc, locations, NoRetryPolicyFactory.INSTANCE);
        testStartWhenStartSucceed();
        WaitForStateSubscriber tempFailSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        WaitForStateSubscriber permFailSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.STOPPED));
        clusterController.jobFinish(listener.getJobId(), JobStatus.FAILURE,
                Collections.singletonList(new HyracksDataException("Runtime Failure")));
        tempFailSubscriber.sync();
        permFailSubscriber.sync();
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testStopDuringRecoveryAttemptThatSucceeds() throws Exception {
        testStartWhenStartSucceed();
        listener.onStart(Behavior.FAIL_COMPILE);
        WaitForStateSubscriber tempFailSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        clusterController.jobFinish(listener.getJobId(), JobStatus.FAILURE,
                Collections.singletonList(new HyracksDataException("Runtime Failure")));
        // recovery is ongoing
        listener.onStart(Behavior.STEP_SUCCEED);
        tempFailSubscriber.sync();
        WaitForStateSubscriber runningSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RUNNING));
        WaitForStateSubscriber stopSubscriber = new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.STOPPED));
        WaitForStateSubscriber recoveringSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RECOVERING));
        recoveringSubscriber.sync();
        listener.onStop(Behavior.SUCCEED);
        Action stopAction = users[0].stopActivity(listener);
        listener.allowStep();
        runningSubscriber.sync();
        stopSubscriber.sync();
        stopAction.sync();
        assertSuccess(stopAction);
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testStopDuringRecoveryAttemptThatFailsCompile() throws Exception {
        testStartWhenStartSucceed();
        listener.onStart(Behavior.FAIL_COMPILE);
        WaitForStateSubscriber tempFailSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        clusterController.jobFinish(listener.getJobId(), JobStatus.FAILURE,
                Collections.singletonList(new HyracksDataException("Runtime Failure")));
        // recovery is ongoing
        listener.onStart(Behavior.STEP_FAIL_COMPILE);
        tempFailSubscriber.sync();
        WaitForStateSubscriber secondTempFailSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        WaitForStateSubscriber stopSubscriber = new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.STOPPED));
        WaitForStateSubscriber recoveringSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RECOVERING));
        recoveringSubscriber.sync();
        listener.onStop(Behavior.SUCCEED);
        Action stopAction = users[0].stopActivity(listener);
        listener.allowStep();
        secondTempFailSubscriber.sync();
        stopSubscriber.sync();
        stopAction.sync();
        assertSuccess(stopAction);
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testStopDuringRecoveryAttemptThatFailsRuntime() throws Exception {
        testStartWhenStartSucceed();
        listener.onStart(Behavior.FAIL_COMPILE);
        WaitForStateSubscriber tempFailSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        clusterController.jobFinish(listener.getJobId(), JobStatus.FAILURE,
                Collections.singletonList(new HyracksDataException("Runtime Failure")));
        // recovery is ongoing
        listener.onStart(Behavior.STEP_FAIL_RUNTIME);
        tempFailSubscriber.sync();
        WaitForStateSubscriber secondTempFailSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        WaitForStateSubscriber stopSubscriber = new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.STOPPED));
        WaitForStateSubscriber recoveringSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RECOVERING));
        recoveringSubscriber.sync();
        listener.onStop(Behavior.SUCCEED);
        Action stopAction = users[0].stopActivity(listener);
        listener.allowStep();
        secondTempFailSubscriber.sync();
        stopSubscriber.sync();
        stopAction.sync();
        assertSuccess(stopAction);
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testStartDuringRecoveryAttemptThatSucceeds() throws Exception {
        testStartWhenStartSucceed();
        listener.onStart(Behavior.FAIL_COMPILE);
        WaitForStateSubscriber tempFailSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        clusterController.jobFinish(listener.getJobId(), JobStatus.FAILURE,
                Collections.singletonList(new HyracksDataException("Runtime Failure")));
        // recovery is ongoing
        listener.onStart(Behavior.STEP_SUCCEED);
        tempFailSubscriber.sync();
        WaitForStateSubscriber runningSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RUNNING));
        WaitForStateSubscriber recoveringSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RECOVERING));
        recoveringSubscriber.sync();
        listener.onStop(Behavior.SUCCEED);
        Action startAction = users[0].startActivity(listener);
        listener.allowStep();
        runningSubscriber.sync();
        startAction.sync();
        assertFailure(startAction, ErrorCode.ACTIVE_ENTITY_ALREADY_STARTED);
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testStartDuringRecoveryAttemptThatFailsCompile() throws Exception {
        testStartWhenStartSucceed();
        listener.onStart(Behavior.FAIL_COMPILE);
        WaitForStateSubscriber tempFailSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        clusterController.jobFinish(listener.getJobId(), JobStatus.FAILURE,
                Collections.singletonList(new HyracksDataException("Runtime Failure")));
        // recovery is ongoing
        listener.onStart(Behavior.STEP_FAIL_COMPILE);
        tempFailSubscriber.sync();
        WaitForStateSubscriber secondTempFailSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        WaitForStateSubscriber recoveringSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RECOVERING));
        recoveringSubscriber.sync();
        listener.onStop(Behavior.SUCCEED);
        Action action = users[0].startActivity(listener);
        listener.allowStep();
        secondTempFailSubscriber.sync();
        action.sync();
        assertFailure(action, ErrorCode.ACTIVE_ENTITY_ALREADY_STARTED);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testStartDuringRecoveryAttemptThatFailsRuntime() throws Exception {
        testStartWhenStartSucceed();
        listener.onStart(Behavior.FAIL_COMPILE);
        WaitForStateSubscriber tempFailSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        clusterController.jobFinish(listener.getJobId(), JobStatus.FAILURE,
                Collections.singletonList(new HyracksDataException("Runtime Failure")));
        // recovery is ongoing
        listener.onStart(Behavior.STEP_FAIL_RUNTIME);
        tempFailSubscriber.sync();
        WaitForStateSubscriber secondTempFailSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        WaitForStateSubscriber recoveringSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RECOVERING));
        recoveringSubscriber.sync();
        Action action = users[0].startActivity(listener);
        listener.allowStep();
        secondTempFailSubscriber.sync();
        action.sync();
        assertFailure(action, ErrorCode.ACTIVE_ENTITY_ALREADY_STARTED);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testSuspendDuringRecoveryAttemptThatSucceedsThenResumeSucceeds() throws Exception {
        testStartWhenStartSucceed();
        listener.onStart(Behavior.FAIL_COMPILE);
        WaitForStateSubscriber tempFailSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        clusterController.jobFinish(listener.getJobId(), JobStatus.FAILURE,
                Collections.singletonList(new HyracksDataException("Runtime Failure")));
        // recovery is ongoing
        listener.onStart(Behavior.STEP_SUCCEED);
        tempFailSubscriber.sync();
        WaitForStateSubscriber runningSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RUNNING));
        WaitForStateSubscriber recoveringSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RECOVERING));
        recoveringSubscriber.sync();
        listener.onStop(Behavior.SUCCEED);
        Action suspend = users[1].suspendActivity(listener);
        listener.allowStep();
        runningSubscriber.sync();
        suspend.sync();
        assertSuccess(suspend);
        Assert.assertEquals(ActivityState.SUSPENDED, listener.getState());
        listener.onStart(Behavior.SUCCEED);
        Action resume = users[1].resumeActivity(listener);
        resume.sync();
        assertSuccess(resume);
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testSuspendDuringRecoveryAttemptThatSucceedsThenResumeFailsCompile() throws Exception {
        testStartWhenStartSucceed();
        listener.onStart(Behavior.FAIL_COMPILE);
        WaitForStateSubscriber tempFailSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        clusterController.jobFinish(listener.getJobId(), JobStatus.FAILURE,
                Collections.singletonList(new HyracksDataException("Runtime Failure")));
        // recovery is ongoing
        listener.onStart(Behavior.STEP_SUCCEED);
        tempFailSubscriber.sync();
        WaitForStateSubscriber runningSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RUNNING));
        WaitForStateSubscriber recoveringSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RECOVERING));
        recoveringSubscriber.sync();
        listener.onStop(Behavior.SUCCEED);
        Action suspend = users[1].suspendActivity(listener);
        listener.allowStep();
        runningSubscriber.sync();
        suspend.sync();
        assertSuccess(suspend);
        Assert.assertEquals(ActivityState.SUSPENDED, listener.getState());
        // Fix here
        listener.onStart(Behavior.FAIL_COMPILE);
        tempFailSubscriber =
                new WaitForStateSubscriber(listener, Collections.singleton(ActivityState.TEMPORARILY_FAILED));
        recoveringSubscriber = new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RECOVERING));
        Action resume = users[1].resumeActivity(listener);
        resume.sync();
        assertSuccess(resume);
        tempFailSubscriber.sync();
        recoveringSubscriber.sync();
        ActivityState state = listener.getState();
        Assert.assertTrue(state == ActivityState.RECOVERING || state == ActivityState.TEMPORARILY_FAILED);
        Assert.assertNotNull(listener.getRecoveryTask());
        runningSubscriber = new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RUNNING));
        listener.onStart(Behavior.SUCCEED);
        runningSubscriber.sync();
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testSuspendDuringRecoveryAttemptThatSucceedsThenResumeFailsRuntime() throws Exception {
        testStartWhenStartSucceed();
        listener.onStart(Behavior.FAIL_COMPILE);
        WaitForStateSubscriber tempFailSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        List<Exception> exceptions = Collections.singletonList(new HyracksDataException("Runtime Failure"));
        clusterController.jobFinish(listener.getJobId(), JobStatus.FAILURE, exceptions);
        // recovery is ongoing
        listener.onStart(Behavior.STEP_SUCCEED);
        tempFailSubscriber.sync();
        WaitForStateSubscriber runningSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RUNNING));
        WaitForStateSubscriber recoveringSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RECOVERING));
        recoveringSubscriber.sync();
        listener.onStop(Behavior.SUCCEED);
        Action suspend = users[1].suspendActivity(listener);
        listener.allowStep();
        runningSubscriber.sync();
        suspend.sync();
        assertSuccess(suspend);
        Assert.assertEquals(ActivityState.SUSPENDED, listener.getState());
        // Fix here
        listener.onStart(Behavior.FAIL_RUNTIME);
        tempFailSubscriber =
                new WaitForStateSubscriber(listener, Collections.singleton(ActivityState.TEMPORARILY_FAILED));
        recoveringSubscriber = new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RECOVERING));
        Action resume = users[1].resumeActivity(listener);
        resume.sync();
        assertSuccess(resume);
        tempFailSubscriber.sync();
        recoveringSubscriber.sync();
        ActivityState state = listener.getState();
        Assert.assertTrue(state == ActivityState.RECOVERING || state == ActivityState.TEMPORARILY_FAILED);
        Assert.assertNotNull(listener.getRecoveryTask());
        runningSubscriber = new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RUNNING));
        listener.onStart(Behavior.SUCCEED);
        runningSubscriber.sync();
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testSuspendDuringRecoveryAttemptThatFailsCompile() throws Exception {
        testStartWhenStartSucceed();
        listener.onStart(Behavior.FAIL_COMPILE);
        WaitForStateSubscriber tempFailSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        clusterController.jobFinish(listener.getJobId(), JobStatus.FAILURE,
                Collections.singletonList(new HyracksDataException("Runtime Failure")));
        // recovery is ongoing
        listener.onStart(Behavior.STEP_FAIL_COMPILE);
        tempFailSubscriber.sync();
        WaitForStateSubscriber recoveringSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RECOVERING));
        recoveringSubscriber.sync();
        Assert.assertEquals(ActivityState.RECOVERING, listener.getState());
        tempFailSubscriber = new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        Action suspend = users[1].suspendActivity(listener);
        listener.onStart(Behavior.FAIL_COMPILE);
        listener.allowStep();
        tempFailSubscriber.sync();
        suspend.sync();
        assertSuccess(suspend);
        Assert.assertEquals(ActivityState.SUSPENDED, listener.getState());
        tempFailSubscriber =
                new WaitForStateSubscriber(listener, Collections.singleton(ActivityState.TEMPORARILY_FAILED));
        recoveringSubscriber = new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RECOVERING));
        Action resume = users[1].resumeActivity(listener);
        resume.sync();
        assertSuccess(resume);
        tempFailSubscriber.sync();
        recoveringSubscriber.sync();
        ActivityState state = listener.getState();
        Assert.assertTrue(state == ActivityState.RECOVERING || state == ActivityState.TEMPORARILY_FAILED);
        Assert.assertNotNull(listener.getRecoveryTask());
        WaitForStateSubscriber runningSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RUNNING));
        listener.onStart(Behavior.SUCCEED);
        runningSubscriber.sync();
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testSuspendDuringRecoveryAttemptThatFailsRuntime() throws Exception {
        testStartWhenStartSucceed();
        listener.onStart(Behavior.FAIL_RUNTIME);
        WaitForStateSubscriber tempFailSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        clusterController.jobFinish(listener.getJobId(), JobStatus.FAILURE,
                Collections.singletonList(new HyracksDataException("Runtime Failure")));
        // recovery is ongoing
        listener.onStart(Behavior.STEP_FAIL_RUNTIME);
        tempFailSubscriber.sync();
        WaitForStateSubscriber recoveringSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RECOVERING));
        recoveringSubscriber.sync();
        Assert.assertEquals(ActivityState.RECOVERING, listener.getState());
        tempFailSubscriber = new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        Action suspend = users[1].suspendActivity(listener);
        listener.onStart(Behavior.FAIL_RUNTIME);
        listener.allowStep();
        tempFailSubscriber.sync();
        suspend.sync();
        assertSuccess(suspend);
        Assert.assertEquals(ActivityState.SUSPENDED, listener.getState());
        tempFailSubscriber =
                new WaitForStateSubscriber(listener, Collections.singleton(ActivityState.TEMPORARILY_FAILED));
        recoveringSubscriber = new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RECOVERING));
        Action resume = users[1].resumeActivity(listener);
        resume.sync();
        assertSuccess(resume);
        tempFailSubscriber.sync();
        recoveringSubscriber.sync();
        ActivityState state = listener.getState();
        Assert.assertTrue(state == ActivityState.RECOVERING || state == ActivityState.TEMPORARILY_FAILED);
        Assert.assertNotNull(listener.getRecoveryTask());
        WaitForStateSubscriber runningSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RUNNING));
        listener.onStart(Behavior.SUCCEED);
        runningSubscriber.sync();
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testCreateNewDatasetDuringRecoveryAttemptThatSucceeds() throws Exception {
        testStartWhenStartSucceed();
        listener.onStart(Behavior.FAIL_COMPILE);
        WaitForStateSubscriber tempFailSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        clusterController.jobFinish(listener.getJobId(), JobStatus.FAILURE,
                Collections.singletonList(new HyracksDataException("Runtime Failure")));
        // recovery is ongoing
        listener.onStart(Behavior.STEP_SUCCEED);
        tempFailSubscriber.sync();
        WaitForStateSubscriber runningSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RUNNING));
        WaitForStateSubscriber recoveringSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RECOVERING));
        recoveringSubscriber.sync();
        Dataset newDataset =
                new Dataset(dataverseName, "newDataset", null, null, null, null, null, null, null, null, 0, 0);
        Action add = users[1].addDataset(newDataset, listener);
        listener.allowStep();
        runningSubscriber.sync();
        add.sync();
        assertFailure(add, ErrorCode.CANNOT_ADD_DATASET_TO_ACTIVE_ENTITY);
        Assert.assertEquals(2, listener.getDatasets().size());
        Assert.assertEquals(clusterController.getAllDatasets().size(), listener.getDatasets().size());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testCreateNewDatasetDuringRecoveryAttemptThatFailsCompile() throws Exception {
        testStartWhenStartSucceed();
        listener.onStart(Behavior.FAIL_COMPILE);
        WaitForStateSubscriber tempFailSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        clusterController.jobFinish(listener.getJobId(), JobStatus.FAILURE,
                Collections.singletonList(new HyracksDataException("Runtime Failure")));
        // recovery is ongoing
        listener.onStart(Behavior.STEP_FAIL_COMPILE);
        tempFailSubscriber.sync();
        WaitForStateSubscriber recoveringSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RECOVERING));
        recoveringSubscriber.sync();
        tempFailSubscriber = new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        Dataset newDataset =
                new Dataset(dataverseName, "newDataset", null, null, null, null, null, null, null, null, 0, 0);
        Action add = users[1].addDataset(newDataset, listener);
        listener.allowStep();
        tempFailSubscriber.sync();
        add.sync();
        assertFailure(add, ErrorCode.CANNOT_ADD_DATASET_TO_ACTIVE_ENTITY);
        Assert.assertEquals(2, listener.getDatasets().size());
        Assert.assertEquals(clusterController.getAllDatasets().size(), listener.getDatasets().size());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testCreateNewDatasetDuringRecoveryAttemptThatFailsRuntime() throws Exception {
        testStartWhenStartSucceed();
        listener.onStart(Behavior.FAIL_COMPILE);
        WaitForStateSubscriber tempFailSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        clusterController.jobFinish(listener.getJobId(), JobStatus.FAILURE,
                Collections.singletonList(new HyracksDataException("Runtime Failure")));
        // recovery is ongoing
        listener.onStart(Behavior.STEP_FAIL_RUNTIME);
        tempFailSubscriber.sync();
        WaitForStateSubscriber recoveringSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RECOVERING));
        recoveringSubscriber.sync();
        tempFailSubscriber = new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        Dataset newDataset =
                new Dataset(dataverseName, "newDataset", null, null, null, null, null, null, null, null, 0, 0);
        Action add = users[1].addDataset(newDataset, listener);
        listener.allowStep();
        tempFailSubscriber.sync();
        add.sync();
        assertFailure(add, ErrorCode.CANNOT_ADD_DATASET_TO_ACTIVE_ENTITY);
        Assert.assertEquals(2, listener.getDatasets().size());
        Assert.assertEquals(clusterController.getAllDatasets().size(), listener.getDatasets().size());
    }

    @Test
    public void testCreateNewDatasetWhileStarting() throws Exception {
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
        listener.onStart(Behavior.STEP_SUCCEED);
        Action startAction = users[0].startActivity(listener);
        WaitForStateSubscriber subscriber =
                new WaitForStateSubscriber(listener, Collections.singleton(ActivityState.STARTING));
        subscriber.sync();
        Dataset newDataset =
                new Dataset(dataverseName, "newDataset", null, null, null, null, null, null, null, null, 0, 0);
        Action createDatasetAction = users[1].addDataset(newDataset, listener);
        listener.allowStep();
        startAction.sync();
        assertSuccess(startAction);
        createDatasetAction.sync();
        assertFailure(createDatasetAction, ErrorCode.CANNOT_ADD_DATASET_TO_ACTIVE_ENTITY);
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
        Assert.assertEquals(2, listener.getDatasets().size());
        Assert.assertEquals(clusterController.getAllDatasets().size(), listener.getDatasets().size());
    }

    @Test
    public void testCreateNewDatasetWhileRunning() throws Exception {
        testStartWhenStartSucceed();
        Dataset newDataset =
                new Dataset(dataverseName, "newDataset", null, null, null, null, null, null, null, null, 0, 0);
        Action createDatasetAction = users[1].addDataset(newDataset, listener);
        createDatasetAction.sync();
        assertFailure(createDatasetAction, ErrorCode.CANNOT_ADD_DATASET_TO_ACTIVE_ENTITY);
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
        Assert.assertEquals(2, listener.getDatasets().size());
        Assert.assertEquals(clusterController.getAllDatasets().size(), listener.getDatasets().size());
    }

    @Test
    public void testCreateNewDatasetWhileSuspended() throws Exception {
        testStartWhenStartSucceed();
        // suspend
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
        listener.onStop(Behavior.STEP_SUCCEED);
        Action suspension = users[1].suspendActivity(listener);
        WaitForStateSubscriber subscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.SUSPENDING, ActivityState.SUSPENDED));
        subscriber.sync();
        Dataset newDataset =
                new Dataset(dataverseName, "newDataset", null, null, null, null, null, null, null, null, 0, 0);
        Action createDatasetAction = users[0].addDataset(newDataset, listener);
        listener.allowStep();
        listener.allowStep();
        suspension.sync();
        assertSuccess(suspension);
        users[1].resumeActivity(listener);
        createDatasetAction.sync();
        assertFailure(createDatasetAction, ErrorCode.CANNOT_ADD_DATASET_TO_ACTIVE_ENTITY);
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
        Assert.assertEquals(2, listener.getDatasets().size());
        Assert.assertEquals(clusterController.getAllDatasets().size(), listener.getDatasets().size());
    }

    @Test
    public void testCreateNewDatasetWhilePermanentFailure() throws Exception {
        testRecoveryFailureAfterOneAttemptCompilationFailure();
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
        Dataset newDataset =
                new Dataset(dataverseName, "newDataset", null, null, null, null, null, null, null, null, 0, 0);
        Action createDatasetAction = users[0].addDataset(newDataset, listener);
        createDatasetAction.sync();
        assertSuccess(createDatasetAction);
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
        Assert.assertEquals(3, listener.getDatasets().size());
        Assert.assertEquals(clusterController.getAllDatasets().size(), listener.getDatasets().size());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testDeleteDatasetDuringRecoveryAttemptThatSucceeds() throws Exception {
        testStartWhenStartSucceed();
        listener.onStart(Behavior.FAIL_COMPILE);
        WaitForStateSubscriber tempFailSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        clusterController.jobFinish(listener.getJobId(), JobStatus.FAILURE,
                Collections.singletonList(new HyracksDataException("Runtime Failure")));
        // recovery is ongoing
        listener.onStart(Behavior.STEP_SUCCEED);
        tempFailSubscriber.sync();
        WaitForStateSubscriber runningSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RUNNING));
        WaitForStateSubscriber recoveringSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RECOVERING));
        recoveringSubscriber.sync();
        Action drop = users[1].dropDataset(firstDataset, listener);
        listener.allowStep();
        runningSubscriber.sync();
        drop.sync();
        assertFailure(drop, ErrorCode.CANNOT_REMOVE_DATASET_FROM_ACTIVE_ENTITY);
        Assert.assertEquals(2, listener.getDatasets().size());
        Assert.assertEquals(clusterController.getAllDatasets().size(), listener.getDatasets().size());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testDeleteDatasetDuringRecoveryAttemptThatFailsCompile() throws Exception {
        testStartWhenStartSucceed();
        listener.onStart(Behavior.FAIL_COMPILE);
        WaitForStateSubscriber tempFailSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        clusterController.jobFinish(listener.getJobId(), JobStatus.FAILURE,
                Collections.singletonList(new HyracksDataException("Runtime Failure")));
        // recovery is ongoing
        listener.onStart(Behavior.STEP_FAIL_COMPILE);
        tempFailSubscriber.sync();
        WaitForStateSubscriber recoveringSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RECOVERING));
        recoveringSubscriber.sync();
        tempFailSubscriber = new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        Action drop = users[1].dropDataset(firstDataset, listener);
        listener.allowStep();
        tempFailSubscriber.sync();
        drop.sync();
        assertFailure(drop, ErrorCode.CANNOT_REMOVE_DATASET_FROM_ACTIVE_ENTITY);
        Assert.assertEquals(2, listener.getDatasets().size());
        Assert.assertEquals(clusterController.getAllDatasets().size(), listener.getDatasets().size());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testDeleteDatasetDuringRecoveryAttemptThatFailsRuntime() throws Exception {
        testStartWhenStartSucceed();
        listener.onStart(Behavior.FAIL_COMPILE);
        WaitForStateSubscriber tempFailSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        clusterController.jobFinish(listener.getJobId(), JobStatus.FAILURE,
                Collections.singletonList(new HyracksDataException("Runtime Failure")));
        // recovery is ongoing
        listener.onStart(Behavior.STEP_FAIL_RUNTIME);
        tempFailSubscriber.sync();
        WaitForStateSubscriber recoveringSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RECOVERING));
        recoveringSubscriber.sync();
        tempFailSubscriber = new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        Action drop = users[1].dropDataset(firstDataset, listener);
        listener.allowStep();
        tempFailSubscriber.sync();
        drop.sync();
        assertFailure(drop, ErrorCode.CANNOT_REMOVE_DATASET_FROM_ACTIVE_ENTITY);
        Assert.assertEquals(2, listener.getDatasets().size());
        Assert.assertEquals(clusterController.getAllDatasets().size(), listener.getDatasets().size());
    }

    @Test
    public void testDeleteDatasetWhileStarting() throws Exception {
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
        listener.onStart(Behavior.STEP_SUCCEED);
        Action startAction = users[0].startActivity(listener);
        WaitForStateSubscriber subscriber =
                new WaitForStateSubscriber(listener, Collections.singleton(ActivityState.STARTING));
        subscriber.sync();
        Action dropDatasetAction = users[1].dropDataset(firstDataset, listener);
        listener.allowStep();
        startAction.sync();
        assertSuccess(startAction);
        dropDatasetAction.sync();
        assertFailure(dropDatasetAction, ErrorCode.CANNOT_REMOVE_DATASET_FROM_ACTIVE_ENTITY);
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
        Assert.assertEquals(2, listener.getDatasets().size());
        Assert.assertEquals(clusterController.getAllDatasets().size(), listener.getDatasets().size());
    }

    @Test
    public void testDeleteDatasetWhileRunning() throws Exception {
        testStartWhenStartSucceed();
        Action dropDatasetAction = users[1].dropDataset(firstDataset, listener);
        dropDatasetAction.sync();
        assertFailure(dropDatasetAction, ErrorCode.CANNOT_REMOVE_DATASET_FROM_ACTIVE_ENTITY);
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
        Assert.assertEquals(2, listener.getDatasets().size());
        Assert.assertEquals(clusterController.getAllDatasets().size(), listener.getDatasets().size());
    }

    @Test
    public void testDeleteDatasetWhilePermanentFailure() throws Exception {
        testRecoveryFailureAfterOneAttemptCompilationFailure();
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
        Action dropDatasetAction = users[0].dropDataset(secondDataset, listener);
        dropDatasetAction.sync();
        assertSuccess(dropDatasetAction);
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
        Assert.assertEquals(1, listener.getDatasets().size());
        Assert.assertEquals(clusterController.getAllDatasets().size(), listener.getDatasets().size());
    }

    @Test
    public void testDeleteDatasetWhileSuspended() throws Exception {
        testStartWhenStartSucceed();
        // suspend
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
        listener.onStop(Behavior.STEP_SUCCEED);
        Action suspension = users[1].suspendActivity(listener);
        WaitForStateSubscriber subscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.SUSPENDING, ActivityState.SUSPENDED));
        subscriber.sync();
        Action dropDatasetAction = users[0].dropDataset(secondDataset, listener);
        listener.allowStep();
        listener.allowStep();
        suspension.sync();
        assertSuccess(suspension);
        users[1].resumeActivity(listener);
        dropDatasetAction.sync();
        assertFailure(dropDatasetAction, ErrorCode.CANNOT_REMOVE_DATASET_FROM_ACTIVE_ENTITY);
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
        Assert.assertEquals(2, listener.getDatasets().size());
        Assert.assertEquals(clusterController.getAllDatasets().size(), listener.getDatasets().size());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testCreateNewIndexDuringRecoveryAttemptThatSucceeds() throws Exception {
        testStartWhenStartSucceed();
        listener.onStart(Behavior.FAIL_COMPILE);
        WaitForStateSubscriber tempFailSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        clusterController.jobFinish(listener.getJobId(), JobStatus.FAILURE,
                Collections.singletonList(new HyracksDataException("Runtime Failure")));
        // recovery is ongoing
        listener.onStart(Behavior.STEP_SUCCEED);
        tempFailSubscriber.sync();
        WaitForStateSubscriber runningSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RUNNING));
        WaitForStateSubscriber recoveringSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RECOVERING));
        recoveringSubscriber.sync();
        Action add = users[1].addIndex(firstDataset, listener);
        listener.allowStep();
        runningSubscriber.sync();
        add.sync();
        assertFailure(add, ErrorCode.CANNOT_ADD_INDEX_TO_DATASET_CONNECTED_TO_ACTIVE_ENTITY);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testCreateNewIndexDuringRecoveryAttemptThatFailsCompile() throws Exception {
        testStartWhenStartSucceed();
        listener.onStart(Behavior.FAIL_COMPILE);
        WaitForStateSubscriber tempFailSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        clusterController.jobFinish(listener.getJobId(), JobStatus.FAILURE,
                Collections.singletonList(new HyracksDataException("Runtime Failure")));
        // recovery is ongoing
        listener.onStart(Behavior.STEP_FAIL_COMPILE);
        tempFailSubscriber.sync();
        WaitForStateSubscriber recoveringSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RECOVERING));
        recoveringSubscriber.sync();
        tempFailSubscriber = new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        Action add = users[1].addIndex(firstDataset, listener);
        listener.allowStep();
        tempFailSubscriber.sync();
        add.sync();
        assertFailure(add, ErrorCode.CANNOT_ADD_INDEX_TO_DATASET_CONNECTED_TO_ACTIVE_ENTITY);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testCreateNewIndexDuringRecoveryAttemptThatFailsRuntime() throws Exception {
        testStartWhenStartSucceed();
        listener.onStart(Behavior.FAIL_RUNTIME);
        WaitForStateSubscriber tempFailSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        clusterController.jobFinish(listener.getJobId(), JobStatus.FAILURE,
                Collections.singletonList(new HyracksDataException("Runtime Failure")));
        // recovery is ongoing
        listener.onStart(Behavior.STEP_FAIL_COMPILE);
        tempFailSubscriber.sync();
        WaitForStateSubscriber recoveringSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RECOVERING));
        recoveringSubscriber.sync();
        tempFailSubscriber = new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        Action add = users[1].addIndex(firstDataset, listener);
        listener.allowStep();
        tempFailSubscriber.sync();
        add.sync();
        assertFailure(add, ErrorCode.CANNOT_ADD_INDEX_TO_DATASET_CONNECTED_TO_ACTIVE_ENTITY);
    }

    @Test
    public void testCreateNewIndexWhileStarting() throws Exception {
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
        listener.onStart(Behavior.STEP_SUCCEED);
        Action startAction = users[0].startActivity(listener);
        WaitForStateSubscriber subscriber =
                new WaitForStateSubscriber(listener, Collections.singleton(ActivityState.STARTING));
        subscriber.sync();
        Action add = users[1].addIndex(firstDataset, listener);
        listener.allowStep();
        startAction.sync();
        assertSuccess(startAction);
        add.sync();
        assertFailure(add, ErrorCode.CANNOT_ADD_INDEX_TO_DATASET_CONNECTED_TO_ACTIVE_ENTITY);
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
    }

    @Test
    public void testCreateNewIndexWhileRunning() throws Exception {
        testStartWhenStartSucceed();
        Action add = users[1].addIndex(firstDataset, listener);
        add.sync();
        assertFailure(add, ErrorCode.CANNOT_ADD_INDEX_TO_DATASET_CONNECTED_TO_ACTIVE_ENTITY);
    }

    @Test
    public void testCreateNewIndexWhilePermanentFailure() throws Exception {
        testRecoveryFailureAfterOneAttemptCompilationFailure();
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
        Action add = users[1].addIndex(firstDataset, listener);
        add.sync();
        assertSuccess(add);
    }

    @Test
    public void testCreateNewIndexWhileSuspended() throws Exception {
        testStartWhenStartSucceed();
        // suspend
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
        listener.onStop(Behavior.STEP_SUCCEED);
        Action suspension = users[1].suspendActivity(listener);
        WaitForStateSubscriber subscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.SUSPENDING, ActivityState.SUSPENDED));
        subscriber.sync();
        Action add = users[0].addIndex(firstDataset, listener);
        listener.allowStep();
        listener.allowStep();
        suspension.sync();
        assertSuccess(suspension);
        users[1].resumeActivity(listener);
        add.sync();
        assertFailure(add, ErrorCode.CANNOT_ADD_INDEX_TO_DATASET_CONNECTED_TO_ACTIVE_ENTITY);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testDeleteIndexDuringRecoveryAttemptThatSucceeds() throws Exception {
        testStartWhenStartSucceed();
        listener.onStart(Behavior.FAIL_COMPILE);
        WaitForStateSubscriber tempFailSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        clusterController.jobFinish(listener.getJobId(), JobStatus.FAILURE,
                Collections.singletonList(new HyracksDataException("Runtime Failure")));
        // recovery is ongoing
        listener.onStart(Behavior.STEP_SUCCEED);
        tempFailSubscriber.sync();
        WaitForStateSubscriber runningSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RUNNING));
        WaitForStateSubscriber recoveringSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RECOVERING));
        recoveringSubscriber.sync();
        Action drop = users[1].dropIndex(firstDataset, listener);
        listener.allowStep();
        runningSubscriber.sync();
        drop.sync();
        assertFailure(drop, ErrorCode.CANNOT_REMOVE_INDEX_FROM_DATASET_CONNECTED_TO_ACTIVE_ENTITY);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testDeleteIndexDuringRecoveryAttemptThatFailsCompile() throws Exception {
        testStartWhenStartSucceed();
        listener.onStart(Behavior.FAIL_COMPILE);
        WaitForStateSubscriber tempFailSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        clusterController.jobFinish(listener.getJobId(), JobStatus.FAILURE,
                Collections.singletonList(new HyracksDataException("Runtime Failure")));
        // recovery is ongoing
        listener.onStart(Behavior.STEP_FAIL_COMPILE);
        tempFailSubscriber.sync();
        WaitForStateSubscriber recoveringSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RECOVERING));
        recoveringSubscriber.sync();
        tempFailSubscriber = new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        Action drop = users[1].dropIndex(firstDataset, listener);
        listener.allowStep();
        tempFailSubscriber.sync();
        drop.sync();
        assertFailure(drop, ErrorCode.CANNOT_REMOVE_INDEX_FROM_DATASET_CONNECTED_TO_ACTIVE_ENTITY);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testDeleteIndexDuringRecoveryAttemptThatFailsRuntime() throws Exception {
        testStartWhenStartSucceed();
        listener.onStart(Behavior.FAIL_COMPILE);
        WaitForStateSubscriber tempFailSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        clusterController.jobFinish(listener.getJobId(), JobStatus.FAILURE,
                Collections.singletonList(new HyracksDataException("Runtime Failure")));
        // recovery is ongoing
        listener.onStart(Behavior.STEP_FAIL_RUNTIME);
        tempFailSubscriber.sync();
        WaitForStateSubscriber recoveringSubscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.RECOVERING));
        recoveringSubscriber.sync();
        tempFailSubscriber = new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.TEMPORARILY_FAILED));
        Action drop = users[1].dropIndex(firstDataset, listener);
        listener.allowStep();
        tempFailSubscriber.sync();
        drop.sync();
        assertFailure(drop, ErrorCode.CANNOT_REMOVE_INDEX_FROM_DATASET_CONNECTED_TO_ACTIVE_ENTITY);
    }

    @Test
    public void testDeleteIndexwWhileStarting() throws Exception {
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
        listener.onStart(Behavior.STEP_SUCCEED);
        Action startAction = users[0].startActivity(listener);
        WaitForStateSubscriber subscriber =
                new WaitForStateSubscriber(listener, Collections.singleton(ActivityState.STARTING));
        subscriber.sync();
        Action drop = users[1].dropIndex(firstDataset, listener);
        listener.allowStep();
        startAction.sync();
        assertSuccess(startAction);
        drop.sync();
        assertFailure(drop, ErrorCode.CANNOT_REMOVE_INDEX_FROM_DATASET_CONNECTED_TO_ACTIVE_ENTITY);
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
    }

    @Test
    public void testDeleteIndexWhileRunning() throws Exception {
        testStartWhenStartSucceed();
        Action drop = users[1].dropIndex(firstDataset, listener);
        drop.sync();
        assertFailure(drop, ErrorCode.CANNOT_REMOVE_INDEX_FROM_DATASET_CONNECTED_TO_ACTIVE_ENTITY);
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
    }

    @Test
    public void testDeleteIndexWhilePermanentFailure() throws Exception {
        testRecoveryFailureAfterOneAttemptCompilationFailure();
        Assert.assertEquals(ActivityState.STOPPED, listener.getState());
        Action drop = users[1].dropIndex(firstDataset, listener);
        drop.sync();
        assertSuccess(drop);
    }

    @Test
    public void testDeleteIndexWhileSuspended() throws Exception {
        testStartWhenStartSucceed();
        // suspend
        Assert.assertEquals(ActivityState.RUNNING, listener.getState());
        listener.onStop(Behavior.STEP_SUCCEED);
        Action suspension = users[1].suspendActivity(listener);
        WaitForStateSubscriber subscriber =
                new WaitForStateSubscriber(listener, EnumSet.of(ActivityState.SUSPENDING, ActivityState.SUSPENDED));
        subscriber.sync();
        Action drop = users[0].dropIndex(firstDataset, listener);
        listener.allowStep();
        listener.allowStep();
        suspension.sync();
        assertSuccess(suspension);
        users[1].resumeActivity(listener);
        drop.sync();
        assertFailure(drop, ErrorCode.CANNOT_REMOVE_INDEX_FROM_DATASET_CONNECTED_TO_ACTIVE_ENTITY);
    }

    @Test
    public void testSuspendedAllActivities() throws Exception {
        TestEventsListener[] additionalListeners = new TestEventsListener[3];
        for (int i = 0; i < additionalListeners.length; i++) {
            String entityName = "entityName" + i;
            EntityId entityId = new EntityId(Feed.EXTENSION_NAME, dataverseName, entityName);
            ClusterControllerService ccService = Mockito.mock(ClusterControllerService.class);
            CCServiceContext ccServiceCtx = Mockito.mock(CCServiceContext.class);
            CcApplicationContext ccAppCtx = Mockito.mock(CcApplicationContext.class);
            IStatementExecutor statementExecutor = Mockito.mock(IStatementExecutor.class);
            IHyracksClientConnection hcc = Mockito.mock(IHyracksClientConnection.class);
            IFunctionExtensionManager functionExtensionManager = Mockito.mock(IFunctionExtensionManager.class);
            Mockito.when(functionExtensionManager.getFunctionManager())
                    .thenReturn(new FunctionManager(FunctionCollection.createDefaultFunctionCollection()));
            Mockito.when(ccAppCtx.getExtensionManager()).thenReturn(functionExtensionManager);
            Mockito.when(ccAppCtx.getActiveNotificationHandler()).thenReturn(handler);
            Mockito.when(ccAppCtx.getMetadataLockManager()).thenReturn(lockManager);
            Mockito.when(ccAppCtx.getServiceContext()).thenReturn(ccServiceCtx);
            Mockito.when(ccAppCtx.getClusterStateManager()).thenReturn(clusterStateManager);
            Mockito.when(ccServiceCtx.getControllerService()).thenReturn(ccService);
            Mockito.when(ccService.getExecutor()).thenReturn(executor);
            Mockito.when(ccAppCtx.getStorageComponentProvider()).thenReturn(componentProvider);
            AlgebricksAbsolutePartitionConstraint locations = new AlgebricksAbsolutePartitionConstraint(nodes);
            additionalListeners[i] = listener = new TestEventsListener(clusterController, nodeControllers, jobIdFactory,
                    entityId, new ArrayList<>(allDatasets), statementExecutor, ccAppCtx, hcc, locations,
                    new InfiniteRetryPolicyFactory());
        }
        Action suspension = users[0].suspendAllActivities(handler);
        suspension.sync();
        assertSuccess(suspension);
        Action query = users[1].query(firstDataset, new Semaphore(1));
        query.sync();
        assertSuccess(query);
        Dataset newDataset =
                new Dataset(dataverseName, "newDataset", null, null, null, null, null, null, null, null, 0, 0);
        Action addDataset = users[1].addDataset(newDataset, listener);
        // blocked by suspension
        Assert.assertFalse(addDataset.isDone());
        Action resumption = users[0].resumeAllActivities(handler);
        resumption.sync();
        assertSuccess(resumption);
        addDataset.sync();
        assertSuccess(addDataset);
    }

    private void assertFailure(Action action, int errorCode) throws Exception {
        HyracksDataException exception = action.getFailure();
        try {
            Assert.assertTrue(action.hasFailed());
            Assert.assertNotNull(exception);
            Assert.assertEquals(errorCode, exception.getErrorCode());
        } catch (Exception e) {
            throw new Exception("Expected failure: " + errorCode + ". Found failure: " + exception);
        }
    }

    private void assertSuccess(Action action) throws Exception {
        if (action.hasFailed()) {
            System.err.println("Action failed while it was expected to succeed");
            action.getFailure().printStackTrace();
            throw action.getFailure();
        }
    }
}
