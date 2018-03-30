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
package org.apache.asterix.test.dataflow;

import java.nio.file.Paths;

import org.apache.asterix.app.bootstrap.TestNodeController;
import org.apache.asterix.app.bootstrap.TestNodeController.PrimaryIndexInfo;
import org.apache.asterix.app.nc.NCAppRuntimeContext;
import org.apache.asterix.common.api.IDatasetLifecycleManager;
import org.apache.asterix.test.base.TestMethodTracer;
import org.apache.asterix.test.common.TestHelper;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.job.JobId;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.DataUtils;
import org.apache.hyracks.storage.am.common.api.IIndexDataflowHelper;
import org.apache.hyracks.storage.am.common.dataflow.IndexDataflowHelperFactory;
import org.apache.hyracks.storage.am.common.freepage.MutableArrayValueReference;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.btree.impl.TestLsmBtree;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.util.ComponentUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

public class FlushMetadataOnlyTest {
    private static TestNodeController nc;
    private static TestLsmBtree lsmBtree;
    private static NCAppRuntimeContext ncAppCtx;
    private static IDatasetLifecycleManager dsLifecycleMgr;
    private static IHyracksTaskContext ctx;
    private static IIndexDataflowHelper indexDataflowHelper;
    private static final int PARTITION = 0;

    @Rule
    public TestRule watcher = new TestMethodTracer();

    @BeforeClass
    public static void setUp() throws Exception {
        TestHelper.deleteExistingInstanceFiles();
        String configPath = Paths.get(System.getProperty("user.dir"), "src", "test", "resources", "cc.conf").toString();
        nc = new TestNodeController(configPath, false);
        nc.init();
        ncAppCtx = nc.getAppRuntimeContext();
        dsLifecycleMgr = ncAppCtx.getDatasetLifecycleManager();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        nc.deInit();
        TestHelper.deleteExistingInstanceFiles();
    }

    @Before
    public void createIndex() throws Exception {
        PrimaryIndexInfo primaryIndexInfo = StorageTestUtils.createPrimaryIndex(nc, PARTITION);
        IndexDataflowHelperFactory iHelperFactory =
                new IndexDataflowHelperFactory(nc.getStorageManager(), primaryIndexInfo.getFileSplitProvider());
        JobId jobId = nc.newJobId();
        ctx = nc.createTestContext(jobId, PARTITION, false);
        indexDataflowHelper = iHelperFactory.create(ctx.getJobletContext().getServiceContext(), PARTITION);
        indexDataflowHelper.open();
        lsmBtree = (TestLsmBtree) indexDataflowHelper.getIndexInstance();
        indexDataflowHelper.close();
    }

    @Test
    public void testFlushMetadataOnlyComponent() throws Exception {
        // allow all operations
        StorageTestUtils.allowAllOps(lsmBtree);
        // ensure no disk component and memory component is empty
        Assert.assertEquals(0, lsmBtree.getDiskComponents().size());
        Assert.assertFalse(lsmBtree.isMemoryComponentsAllocated());
        MutableArrayValueReference key = new MutableArrayValueReference("FlushMetadataOnlyTestKey".getBytes());
        MutableArrayValueReference value = new MutableArrayValueReference("FlushMetadataOnlyTestValue".getBytes());
        indexDataflowHelper.open();
        ILSMIndexAccessor accessor = lsmBtree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
        accessor.updateMeta(key, value);
        Assert.assertTrue(lsmBtree.isMemoryComponentsAllocated());
        Assert.assertTrue(lsmBtree.getCurrentMemoryComponent().isModified());
        indexDataflowHelper.close();
        // flush synchronously
        StorageTestUtils.flush(dsLifecycleMgr, lsmBtree, false);
        // assert one disk component
        Assert.assertEquals(1, lsmBtree.getDiskComponents().size());
        ArrayBackedValueStorage pointable = new ArrayBackedValueStorage();
        ComponentUtils.get(lsmBtree, key, pointable);
        Assert.assertTrue(DataUtils.equals(pointable, value));
        // ensure that we can search this component
        StorageTestUtils.searchAndAssertCount(nc, PARTITION, 0);
    }

    @After
    public void destroyIndex() throws Exception {
        indexDataflowHelper.destroy();
    }
}
