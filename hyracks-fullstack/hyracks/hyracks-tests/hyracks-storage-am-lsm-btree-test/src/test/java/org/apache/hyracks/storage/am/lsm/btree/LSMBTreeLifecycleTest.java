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
package org.apache.hyracks.storage.am.lsm.btree;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.storage.am.btree.OrderedIndexTestUtils;
import org.apache.hyracks.storage.am.common.AbstractIndexLifecycleTest;
import org.apache.hyracks.storage.am.common.CheckTuple;
import org.apache.hyracks.storage.am.common.IIndexTestContext;
import org.apache.hyracks.storage.am.common.TreeIndexTestUtils;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeTestContext;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeTestHarness;

public class LSMBTreeLifecycleTest extends AbstractIndexLifecycleTest {

    @SuppressWarnings("rawtypes")
    private final ISerializerDeserializer[] fieldSerdes =
            new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE };
    private final LSMBTreeTestHarness harness = new LSMBTreeTestHarness();
    private final TreeIndexTestUtils titu = new OrderedIndexTestUtils();

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
        return ((LSMBTree) index).isEmptyIndex();
    }

    @Override
    public void setup() throws Exception {
        harness.setUp();
        testCtx = LSMBTreeTestContext.create(harness.getIOManager(), harness.getVirtualBufferCaches(),
                harness.getFileReference(), harness.getDiskBufferCache(), fieldSerdes, fieldSerdes.length,
                harness.getBoomFilterFalsePositiveRate(), harness.getMergePolicy(), harness.getOperationTracker(),
                harness.getIOScheduler(), harness.getIOOperationCallbackFactory(),
                harness.getPageWriteCallbackFactory(), harness.getMetadataPageManagerFactory(), false, true, false);
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
