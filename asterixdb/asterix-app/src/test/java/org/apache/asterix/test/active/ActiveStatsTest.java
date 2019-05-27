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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.asterix.active.ActiveRuntimeId;
import org.apache.asterix.active.ActivityState;
import org.apache.asterix.active.EntityId;
import org.apache.asterix.active.IActiveRuntime;
import org.apache.asterix.active.NoRetryPolicyFactory;
import org.apache.asterix.active.message.ActivePartitionMessage;
import org.apache.asterix.active.message.ActivePartitionMessage.Event;
import org.apache.asterix.algebra.base.ILangExtension.Language;
import org.apache.asterix.app.active.ActiveEntityEventsListener;
import org.apache.asterix.app.active.ActiveNotificationHandler;
import org.apache.asterix.app.cc.CCExtensionManager;
import org.apache.asterix.app.nc.NCAppRuntimeContext;
import org.apache.asterix.app.result.ResponsePrinter;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.feed.watch.WaitForStateSubscriber;
import org.apache.asterix.external.operators.FeedIntakeOperatorNodePushable;
import org.apache.asterix.metadata.declared.MetadataProvider;
import org.apache.asterix.metadata.entities.Dataset;
import org.apache.asterix.runtime.utils.CcApplicationContext;
import org.apache.asterix.test.runtime.ExecutionTestUtil;
import org.apache.asterix.translator.IStatementExecutor;
import org.apache.asterix.translator.SessionOutput;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.api.job.JobSpecification;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ActiveStatsTest {

    protected boolean cleanUp = true;
    private static String EXPECTED_STATS = "\"Mock stats\"";
    private static String CONF_PATH = "src/main/resources/cc.conf";

    @Before
    public void setUp() throws Exception {
        ExecutionTestUtil.setUp(cleanUp, CONF_PATH);
    }

    @Test
    public void refreshStatsTest() throws Exception {
        // Entities to be used
        EntityId entityId = new EntityId("MockExtension", "MockDataverse", "MockEntity");
        ActiveRuntimeId activeRuntimeId =
                new ActiveRuntimeId(entityId, FeedIntakeOperatorNodePushable.class.getSimpleName(), 0);
        List<Dataset> datasetList = new ArrayList<>();
        AlgebricksAbsolutePartitionConstraint partitionConstraint =
                new AlgebricksAbsolutePartitionConstraint(new String[] { "asterix_nc1" });
        String requestedStats;
        CcApplicationContext appCtx =
                (CcApplicationContext) ExecutionTestUtil.integrationUtil.cc.getApplicationContext();
        ActiveNotificationHandler activeJobNotificationHandler =
                (ActiveNotificationHandler) appCtx.getActiveNotificationHandler();
        JobId jobId = new JobId(1);

        // Mock ActiveRuntime
        IActiveRuntime mockRuntime = Mockito.mock(IActiveRuntime.class);
        Mockito.when(mockRuntime.getRuntimeId()).thenReturn(activeRuntimeId);
        Mockito.when(mockRuntime.getStats()).thenReturn(EXPECTED_STATS);

        // Mock JobSpecification
        JobSpecification jobSpec = Mockito.mock(JobSpecification.class);
        Mockito.when(jobSpec.getProperty(ActiveNotificationHandler.ACTIVE_ENTITY_PROPERTY_NAME)).thenReturn(entityId);

        // Mock MetadataProvider
        CCExtensionManager extensionManager = (CCExtensionManager) appCtx.getExtensionManager();
        SessionOutput sessionOutput = Mockito.mock(SessionOutput.class);
        IStatementExecutor statementExecutor = extensionManager
                .getStatementExecutorFactory(appCtx.getServiceContext().getControllerService().getExecutor())
                .create(appCtx, Collections.emptyList(), sessionOutput,
                        extensionManager.getCompilationProvider(Language.SQLPP), appCtx.getStorageComponentProvider(),
                        new ResponsePrinter(sessionOutput));
        MetadataProvider mdProvider = new MetadataProvider(appCtx, null);
        // Add event listener
        ActiveEntityEventsListener eventsListener = new DummyFeedEventsListener(statementExecutor, appCtx, null,
                entityId, datasetList, partitionConstraint, FeedIntakeOperatorNodePushable.class.getSimpleName(),
                NoRetryPolicyFactory.INSTANCE, null, Collections.emptyList());
        // Register mock runtime
        NCAppRuntimeContext nc1AppCtx =
                (NCAppRuntimeContext) ExecutionTestUtil.integrationUtil.ncs[0].getApplicationContext();
        nc1AppCtx.getActiveManager().registerRuntime(mockRuntime);

        // Check init stats
        requestedStats = eventsListener.getStats();
        Assert.assertTrue(requestedStats.contains("N/A"));

        // Update stats of not-started job
        eventsListener.refreshStats(1000);
        requestedStats = eventsListener.getStats();
        Assert.assertTrue(requestedStats.contains("N/A"));
        WaitForStateSubscriber startingSubscriber =
                new WaitForStateSubscriber(eventsListener, Collections.singleton(ActivityState.STARTING));
        // Update stats of created/started job without joined partition
        TestUserActor user = new TestUserActor("Xikui", mdProvider, null);
        Action start = user.startActivity(eventsListener);
        startingSubscriber.sync();
        activeJobNotificationHandler.notifyJobCreation(jobId, jobSpec);
        activeJobNotificationHandler.notifyJobStart(jobId);
        eventsListener.refreshStats(1000);
        requestedStats = eventsListener.getStats();
        Assert.assertTrue(requestedStats.contains("N/A"));
        // Fake partition message and notify eventListener
        ActivePartitionMessage partitionMessage =
                new ActivePartitionMessage(activeRuntimeId, jobId, Event.RUNTIME_REGISTERED, null);
        partitionMessage.handle(appCtx);
        start.sync();
        if (start.hasFailed()) {
            throw start.getFailure();
        }
        eventsListener.refreshStats(100000);
        requestedStats = eventsListener.getStats();
        if (!requestedStats.contains(EXPECTED_STATS)) {
            throw new Exception("Expected stats to contain " + EXPECTED_STATS + " but found stats = " + requestedStats);
        }
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            objectMapper.readTree(requestedStats);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }
        // Ask for runtime that is not registered
        HyracksDataException expectedException = null;
        nc1AppCtx.getActiveManager().deregisterRuntime(activeRuntimeId);
        try {
            eventsListener.refreshStats(100000);
        } catch (HyracksDataException e) {
            expectedException = e;
        }
        Assert.assertNotNull(expectedException);
        Assert.assertEquals(ErrorCode.ACTIVE_MANAGER_INVALID_RUNTIME, expectedException.getErrorCode());
    }

}
