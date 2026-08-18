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
package org.apache.hyracks.storage.am.lsm.vector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVTree;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVTreeTestContext;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVTreeTestHarness;
import org.apache.hyracks.storage.am.lsm.vector.util.VectorIndexTestDriver;
import org.apache.hyracks.storage.am.vector.AbstractVectorTreeTestContext;
import org.apache.hyracks.storage.am.vector.VectorTreeTestUtils;
import org.apache.hyracks.storage.common.IIndexBulkLoader;
import org.apache.hyracks.util.annotations.AiProvenance;
import org.junit.After;
import org.junit.Before;

/**
 * A data bulk load that receives NO tuples must still finalize cleanly.
 * <p>
 * This is not a hypothetical shape: CREATE INDEX runs one bulk load per storage partition, and a
 * collection with fewer rows than partitions (or any skewed hash distribution) leaves some partition
 * with nothing to load. That partition still copies the shared static structure in {@code end()}.
 * <p>
 * The copy reserves a contiguous run of page ids. Reserving it with {@code IPageManager.takeBlock()}
 * is wrong here: unlike {@code takePage()}, it does not bind the metadata frame to the metadata page
 * before reading {@code getMaxPage()}, so it only works when the caller has already taken a page. An
 * empty load never takes one, and the call died with
 * {@code NullPointerException: Cannot invoke "java.nio.ByteBuffer.getInt(int)" because "this.buf" is null}
 * inside {@code LIFOMetaDataFrame.getMaxPage}. The failure then compounded: the rolled-back CREATE
 * INDEX left an I/O operation outstanding and the node wedged in {@code DatasetInfo.waitForIO()}.
 * <p>
 * Every other fixture in this suite populates all of its clusters, which is why this went unnoticed.
 */
@AiProvenance(agent = AiProvenance.Agent.CLAUDE_OPUS_5, tool = AiProvenance.Tool.CLAUDE_CODE_UI, contributionKind = AiProvenance.ContributionKind.TEST_GENERATED, notes = "Regression test for the empty-partition bulk load")
public class LSMVTreeEmptyPartitionBulkLoadTest extends VectorIndexTestDriver {

    private final LSMVTreeTestHarness harness = new LSMVTreeTestHarness();
    private final VectorTreeTestUtils testUtils = new VectorTreeTestUtils();

    @Before
    public void setUp() throws HyracksDataException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

    @Override
    protected void runTest(ISerializerDeserializer[] centroidSerdes, ISerializerDeserializer[] dataRecordSerdes,
            List<ITupleReference> centroids, List<Integer> numClustersPerLevel, List<List<Integer>> centroidsPerCluster,
            int vectorDimension, List<List<ITupleReference>> leafRecords) throws Exception {

        AbstractVectorTreeTestContext ctx = LSMVTreeTestContext.create(harness.getNcConfig(), harness.getIOManager(),
                harness.getVirtualBufferCaches(), harness.getFileReference(), harness.getDiskBufferCache(),
                dataRecordSerdes, vectorDimension, harness.getMergePolicy(), harness.getOperationTracker(),
                harness.getIOScheduler(), harness.getIOOperationCallbackFactory(),
                harness.getPageWriteCallbackFactory(), harness.getMetadataPageManagerFactory());

        ctx.setStaticStructureCentroids(centroids);
        ctx.setNumClustersPerLevel(numClustersPerLevel);
        ctx.setNumCentroidsPerLevel(centroidsPerCluster);

        try {
            ctx.getIndex().create();
            ctx.getIndex().activate();

            // The static structure is built exactly as a populated partition would build it -- every
            // partition of a CREATE INDEX shares it, whether or not it received rows.
            testUtils.buildStaticStructure(ctx);

            // A data bulk load that adds nothing, then finalizes. end() must not throw: before the fix it
            // raised an NPE from the page manager, which failed the whole CREATE INDEX job.
            LSMVTree lsmvTree = (LSMVTree) ctx.getIndex();
            Map<String, Object> parameters = new HashMap<>();
            IIndexBulkLoader emptyLoader = lsmvTree.createBulkLoader(1.0f, false, 0L, parameters);
            emptyLoader.end();

            // ...and the component it wrote must be well-formed, not merely not-thrown. Deactivating and
            // reactivating forces the component to be closed and read back from disk, which is what the
            // rolled-back CREATE INDEX choked on. (scanClosestLeafCluster is not usable here: it asserts
            // that records are found, and an empty partition has none by construction.)
            ctx.getIndex().deactivate();
            ctx.getIndex().activate();
        } finally {
            ctx.getIndex().deactivate();
            ctx.getIndex().destroy();
        }
    }
}
