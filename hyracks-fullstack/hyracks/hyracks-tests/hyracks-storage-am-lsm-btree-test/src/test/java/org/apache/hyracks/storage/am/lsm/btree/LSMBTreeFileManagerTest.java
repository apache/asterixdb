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

import java.io.File;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeWithBloomFilterDiskComponent;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeTestContext;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeTestHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LSMBTreeFileManagerTest {

    private final LSMBTreeTestHarness harness = new LSMBTreeTestHarness();

    @Before
    public void setUp() throws HyracksDataException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

    @Test
    public void deleteOrphanedFilesTest() throws Exception {
        ISerializerDeserializer[] fieldSerdes = { IntegerSerializerDeserializer.INSTANCE };
        LSMBTreeTestContext ctx = LSMBTreeTestContext.create(harness.getIOManager(), harness.getVirtualBufferCaches(),
                harness.getFileReference(), harness.getDiskBufferCache(), fieldSerdes, 1,
                harness.getBoomFilterFalsePositiveRate(), harness.getMergePolicy(), harness.getOperationTracker(),
                harness.getIOScheduler(), harness.getIOOperationCallbackFactory(),
                harness.getPageWriteCallbackFactory(), harness.getMetadataPageManagerFactory(), false, true, false);
        ctx.getIndex().create();
        ctx.getIndex().activate();

        // Insert a tuple
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(ctx.getFieldCount());
        for (int i = 0; i < ctx.getFieldCount(); i++) {
            tupleBuilder.addField(fieldSerdes[i], 1);
        }
        ArrayTupleReference tuple = new ArrayTupleReference();
        tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
        ILSMIndexAccessor accessor = (ILSMIndexAccessor) ctx.getIndexAccessor();
        accessor.insert(tuple);

        // Flush to generate a disk component. This uses synchronous scheduler
        accessor.scheduleFlush();

        // Make sure the disk component was generated
        LSMBTree btree = (LSMBTree) ctx.getIndex();
        Assert.assertEquals("Check disk components", 1, btree.getDiskComponents().size());

        ctx.getIndex().deactivate();

        // Delete the btree file and keep the bloom filter file from the disk component
        LSMBTreeWithBloomFilterDiskComponent ilsmDiskComponent =
                (LSMBTreeWithBloomFilterDiskComponent) btree.getDiskComponents().get(0);
        ilsmDiskComponent.getIndex().getFileReference().delete();

        File bloomFilterFile = ilsmDiskComponent.getBloomFilter().getFileReference().getFile().getAbsoluteFile();
        Assert.assertEquals("Check bloom filter file exists", true, bloomFilterFile.exists());

        // Activating the index again should delete the orphaned bloom filter file as well as the disk component
        ctx.getIndex().activate();
        Assert.assertEquals("Check bloom filter file deleted", false, bloomFilterFile.exists());
        Assert.assertEquals("Check disk components", 0, btree.getDiskComponents().size());
    }
}
