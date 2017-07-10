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
package org.apache.hyracks.storage.am.lsm.rtree;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.storage.am.common.AbstractIndexLifecycleTest;
import org.apache.hyracks.storage.am.common.CheckTuple;
import org.apache.hyracks.storage.am.common.IIndexTestContext;
import org.apache.hyracks.storage.am.common.TreeIndexTestUtils;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.lsm.rtree.impls.LSMRTree;
import org.apache.hyracks.storage.am.lsm.rtree.util.LSMRTreeTestContext;
import org.apache.hyracks.storage.am.lsm.rtree.util.LSMRTreeTestHarness;
import org.apache.hyracks.storage.am.rtree.RTreeTestUtils;
import org.apache.hyracks.storage.am.rtree.frames.RTreePolicyType;
import org.apache.hyracks.storage.am.rtree.util.RTreeUtils;

public class LSMRTreeLifecycleTest extends AbstractIndexLifecycleTest {

    @SuppressWarnings("rawtypes")
    private final ISerializerDeserializer[] fieldSerdes = { IntegerSerializerDeserializer.INSTANCE,
            IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
            IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };
    private final IPrimitiveValueProviderFactory[] valueProviderFactories =
            RTreeUtils.createPrimitiveValueProviderFactories(4, IntegerPointable.FACTORY);
    private final int numKeys = 4;

    private final LSMRTreeTestHarness harness = new LSMRTreeTestHarness();
    private final TreeIndexTestUtils titu = new RTreeTestUtils();

    @SuppressWarnings("rawtypes")
    private IIndexTestContext<? extends CheckTuple> testCtx;

    @Override
    protected boolean persistentStateExists() throws Exception {
        // make sure all of the directories exist
        if (!harness.getFileReference().getFile().exists()) {
            return false;
        }
        return true;
    }

    @Override
    protected boolean isEmptyIndex() throws Exception {
        return ((LSMRTree) index).isEmptyIndex();
    }

    @Override
    public void setup() throws Exception {
        harness.setUp();
        testCtx = LSMRTreeTestContext.create(harness.getIOManager(), harness.getVirtualBufferCaches(),
                harness.getFileReference(), harness.getDiskBufferCache(), fieldSerdes, valueProviderFactories, numKeys,
                RTreePolicyType.RTREE, harness.getBoomFilterFalsePositiveRate(), harness.getMergePolicy(),
                harness.getOperationTracker(), harness.getIOScheduler(), harness.getIOOperationCallback(),
                harness.getMetadataPageManagerFactory());
        index = testCtx.getIndex();
    }

    @Override
    public void tearDown() throws Exception {
        harness.tearDown();
    }

    @Override
    protected void performInsertions() throws Exception {
        titu.insertIntTuples(testCtx, 10, harness.getRandom());
    }

    @Override
    protected void checkInsertions() throws Exception {
        titu.checkScan(testCtx);
    }

    @Override
    protected void clearCheckableInsertions() throws Exception {
        testCtx.getCheckTuples().clear();
    }
}
