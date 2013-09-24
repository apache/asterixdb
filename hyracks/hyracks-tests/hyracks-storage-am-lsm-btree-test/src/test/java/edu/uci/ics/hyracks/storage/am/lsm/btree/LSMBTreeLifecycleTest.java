/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.hyracks.storage.am.lsm.btree;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.btree.OrderedIndexTestUtils;
import edu.uci.ics.hyracks.storage.am.common.AbstractIndexLifecycleTest;
import edu.uci.ics.hyracks.storage.am.common.CheckTuple;
import edu.uci.ics.hyracks.storage.am.common.IIndexTestContext;
import edu.uci.ics.hyracks.storage.am.common.TreeIndexTestUtils;
import edu.uci.ics.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import edu.uci.ics.hyracks.storage.am.lsm.btree.util.LSMBTreeTestContext;
import edu.uci.ics.hyracks.storage.am.lsm.btree.util.LSMBTreeTestHarness;

public class LSMBTreeLifecycleTest extends AbstractIndexLifecycleTest {

    @SuppressWarnings("rawtypes")
    private final ISerializerDeserializer[] fieldSerdes = new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE };
    private final LSMBTreeTestHarness harness = new LSMBTreeTestHarness();
    private final TreeIndexTestUtils titu = new OrderedIndexTestUtils();

    @SuppressWarnings("rawtypes")
    private IIndexTestContext<? extends CheckTuple> testCtx;

    @Override
    protected boolean persistentStateExists() throws Exception {
        // make sure all of the directories exist
        if (!new FileReference(harness.getFileReference().getFile()).getFile().exists()) {
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
        testCtx = LSMBTreeTestContext.create(harness.getVirtualBufferCaches(), harness.getFileReference(),
                harness.getDiskBufferCache(), harness.getDiskFileMapProvider(), fieldSerdes, fieldSerdes.length,
                harness.getBoomFilterFalsePositiveRate(), harness.getMergePolicy(), harness.getOperationTracker(),
                harness.getIOScheduler(), harness.getIOOperationCallback());
        index = testCtx.getIndex();
    }

    @Override
    public void tearDown() throws Exception {
        try {
            index.deactivate();
        } catch (Exception e) {
        } finally {
            index.destroy();
        }
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
