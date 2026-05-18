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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.marshalling.ByteArraySerializerDeserializer;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrameFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVTree;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVTreeDiskComponent;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVTreeTestContext;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVTreeTestHarness;
import org.apache.hyracks.storage.am.lsm.vector.util.VectorTestStructure;
import org.apache.hyracks.storage.am.lsm.vector.util.VectorTestStructure.BulkLoadRecordFormat;
import org.apache.hyracks.storage.am.vector.AbstractVectorTreeTestContext;
import org.apache.hyracks.storage.am.vector.VectorTreeTestUtils;
import org.apache.hyracks.storage.am.vector.api.IVTreeInteriorFrame;
import org.apache.hyracks.storage.am.vector.api.IVTreeLeafFrame;
import org.apache.hyracks.storage.am.vector.api.VTreeQuantizationParams;
import org.apache.hyracks.storage.am.vector.impls.VTree;
import org.apache.hyracks.storage.am.vector.utils.VTreeLeafNeighborList;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.ICachedPage;
import org.apache.hyracks.storage.common.file.BufferedFileHandle;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies that a built VTree carries a resolved graph-neighbor list on each leaf centroid.
 * <p>
 * The static-structure builder places leaf centroids with <em>provisional</em> neighbor entries
 * ({@code [centroidId, SENTINEL]}); {@code VTreeBulkLoader} then resolves them to physical pointers
 * ({@code [pageId, slot]}) while copying the static structure into the queryable disk component, where
 * every leaf's final placement is known. Both tests read that committed component:
 * <ul>
 *   <li>{@link #testLeafNeighborListsAreResolvedToPhysicalPointers()} — every neighbor entry resolves
 *       to the exact {@code (pageId, slot)} of its neighbor centroid's leaf placement.</li>
 *   <li>{@link #testLeavesWithoutNeighborsHaveEmptyLists()} — a leaf with no neighbors carries an empty
 *       list (the path production exercises today, before upstream neighbor computation exists).</li>
 * </ul>
 */
public class LSMVTreeLeafNeighborTest {

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

    /** Leaf tuple field index of the neighbor list: [cid, embedding, quantizedBytes, neighborList, metadataPtr]. */
    private static final int NEIGHBOR_FIELD_INDEX = 3;

    /** Recovered placement of one leaf centroid plus the raw bytes of its neighbor field. */
    private static final class LeafReadback {
        final Map<Integer, int[]> placedAt = new HashMap<>(); // centroidId -> {pageId, slot}
        final Map<Integer, byte[]> neighborBytes = new HashMap<>(); // centroidId -> decoded neighbor list
    }

    /**
     * A small deterministic neighbor topology over the 16 leaf centroids (c4..c19) of
     * {@link VectorTestStructure#twoDim2Level()}: each leaf points to the next two leaf ids, wrapping.
     */
    private static Map<Integer, int[]> buildNeighborTopology(VectorTestStructure structure) {
        int first = structure.getFirstLeafCentroidId();
        int n = structure.getNumLeafCentroids();
        Map<Integer, int[]> map = new HashMap<>();
        for (int i = 0; i < n; i++) {
            int cid = first + i;
            int n1 = first + (i + 1) % n;
            int n2 = first + (i + 2) % n;
            map.put(cid, new int[] { n1, n2 });
        }
        return map;
    }

    @Test
    public void testLeavesWithoutNeighborsHaveEmptyLists() throws Exception {
        VectorTestStructure structure = VectorTestStructure.twoDim2Level();
        int first = structure.getFirstLeafCentroidId();
        // Only the first leaf gets neighbors; every other leaf is absent from the map -> empty list.
        Map<Integer, int[]> neighbors = new HashMap<>();
        neighbors.put(first, new int[] { first + 1, first + 2 });
        LeafReadback readback = buildAndReadLeaves(structure, neighbors);

        byte[] withNeighbors = readback.neighborBytes.get(first);
        assertFalse("No leaf record read back for centroid " + first, withNeighbors == null);
        assertEquals("Neighbor count for centroid " + first, 2,
                VTreeLeafNeighborList.entryCount(withNeighbors, 0, withNeighbors.length));
        assertTrue("Neighbor of centroid " + first + " should be resolved",
                VTreeLeafNeighborList.isResolved(withNeighbors, 0, 0));

        // A leaf with no configured neighbors must still carry a (well-formed, empty) neighbor list.
        int bare = first + 5;
        byte[] empty = readback.neighborBytes.get(bare);
        assertFalse("No leaf record read back for centroid " + bare, empty == null);
        assertEquals("Leaf " + bare + " should have an empty neighbor list", 0,
                VTreeLeafNeighborList.entryCount(empty, 0, empty.length));
    }

    @Test
    public void testLeafNeighborListsAreResolvedToPhysicalPointers() throws Exception {
        VectorTestStructure structure = VectorTestStructure.twoDim2Level();
        Map<Integer, int[]> neighbors = buildNeighborTopology(structure);
        LeafReadback readback = buildAndReadLeaves(structure, neighbors);

        for (Map.Entry<Integer, int[]> e : neighbors.entrySet()) {
            int cid = e.getKey();
            int[] expected = e.getValue();
            byte[] buf = readback.neighborBytes.get(cid);
            assertFalse("No leaf record read back for centroid " + cid, buf == null);
            for (int k = 0; k < expected.length; k++) {
                int neighborCid = expected[k];
                int[] target = readback.placedAt.get(neighborCid);
                assertFalse("Neighbor centroid " + neighborCid + " was never placed", target == null);
                assertTrue("Entry " + k + " for centroid " + cid + " must be resolved to a physical pointer",
                        VTreeLeafNeighborList.isResolved(buf, 0, k));
                assertEquals("Resolved pageId (entry " + k + ") for centroid " + cid, target[0],
                        VTreeLeafNeighborList.readPageId(buf, 0, k));
                assertEquals("Resolved slot (entry " + k + ") for centroid " + cid, target[1],
                        VTreeLeafNeighborList.readSlot(buf, 0, k));
            }
        }
    }

    /** Build a static structure with neighbor-bearing leaves, then BFS the leaf pages and read them back. */
    private LeafReadback buildAndReadLeaves(VectorTestStructure structure, Map<Integer, int[]> neighbors)
            throws Exception {
        ISerializerDeserializer[] dataRecordSerdes = structure.getDataRecordSerdes(BulkLoadRecordFormat.NAIVE);
        // Non-null params select the quantized (neighbor-capable) leaf-frame schema; values are unused
        // here because this test only builds the static structure (no data load / search).
        VTreeQuantizationParams quantizationParams = new VTreeQuantizationParams(0f, 1f, 0.9f, 0.999f, 8, 0);
        AbstractVectorTreeTestContext ctx = LSMVTreeTestContext.createWithQuantizedLeafFrame(harness.getNcConfig(),
                harness.getIOManager(), harness.getVirtualBufferCaches(), harness.getFileReference(),
                harness.getDiskBufferCache(), dataRecordSerdes, structure.getVectorDimension(),
                harness.getMergePolicy(), harness.getOperationTracker(), harness.getIOScheduler(),
                harness.getIOOperationCallbackFactory(), harness.getPageWriteCallbackFactory(),
                harness.getMetadataPageManagerFactory(), quantizationParams);

        ctx.setNumClustersPerLevel(structure.getNumClustersPerLevel());
        ctx.setNumCentroidsPerLevel(structure.getCentroidsPerCluster());
        ctx.setStaticStructureCentroids(structure.buildCentroidTuplesWithLeafNeighbors(neighbors));
        // A queryable disk component (whose leaf pages carry the copied neighbor fields) is committed
        // only by the data load, so feed a few records per centroid as well.
        ctx.setDataRecords(structure.generateBulkLoadRecords(BulkLoadRecordFormat.NAIVE, 4));

        LSMVTree lsmvTree = (LSMVTree) ctx.getIndex();
        try {
            lsmvTree.create();
            lsmvTree.activate();
            testUtils.buildStaticStructure(ctx);
            testUtils.bulkLoadRecords(ctx);

            List<ILSMDiskComponent> components = lsmvTree.getDiskComponents();
            assertFalse("Static structure build produced no disk component", components.isEmpty());
            VTree vtree = ((LSMVTreeDiskComponent) components.get(0)).getIndex();

            LeafReadback readback = new LeafReadback();
            readLeafPages(vtree, readback);
            return readback;
        } finally {
            lsmvTree.deactivate();
            lsmvTree.destroy();
        }
    }

    /** BFS the static structure from the root, recording each leaf centroid's (page, slot) and neighbor bytes. */
    private void readLeafPages(VTree vtree, LeafReadback out) throws HyracksDataException {
        IBufferCache bufferCache = vtree.getBufferCache();
        int fileId = vtree.getFileId();
        ITreeIndexFrameFactory interiorFrameFactory = vtree.getInteriorFrameFactory();
        ITreeIndexFrameFactory leafFrameFactory = vtree.getLeafFrameFactory();

        Queue<Integer> queue = new ArrayDeque<>();
        Set<Integer> visited = new HashSet<>();
        int rootPageId = vtree.getRootPageId();
        queue.add(rootPageId);
        visited.add(rootPageId);

        while (!queue.isEmpty()) {
            int pageId = queue.poll();
            ICachedPage page = bufferCache.pin(BufferedFileHandle.getDiskPageId(fileId, pageId));
            page.acquireReadLatch();
            try {
                IVTreeLeafFrame leafFrame = (IVTreeLeafFrame) leafFrameFactory.createFrame();
                leafFrame.setPage(page);
                if (leafFrame.isLeaf()) {
                    int tupleCount = leafFrame.getTupleCount();
                    ITreeIndexTupleReference frameTuple = leafFrame.createTupleReference();
                    for (int i = 0; i < tupleCount; i++) {
                        frameTuple.resetByTupleIndex(leafFrame, i);
                        int cid = leafFrame.getCentroidId(i);
                        out.placedAt.put(cid, new int[] { pageId, i });
                        out.neighborBytes.put(cid, readNeighborField(frameTuple));
                    }
                    if (leafFrame.getOverflowFlagBit()) {
                        int next = leafFrame.getNextLeaf();
                        if (next != -1 && visited.add(next)) {
                            queue.add(next);
                        }
                    }
                } else {
                    IVTreeInteriorFrame interiorFrame = (IVTreeInteriorFrame) interiorFrameFactory.createFrame();
                    interiorFrame.setPage(page);
                    int tupleCount = interiorFrame.getTupleCount();
                    for (int i = 0; i < tupleCount; i++) {
                        int child = interiorFrame.getChildPageId(i);
                        if (child != -1 && visited.add(child)) {
                            queue.add(child);
                        }
                    }
                    if (interiorFrame.getOverflowFlagBit()) {
                        int next = interiorFrame.getNextPage();
                        if (next != -1 && visited.add(next)) {
                            queue.add(next);
                        }
                    }
                }
            } finally {
                page.releaseReadLatch();
                bufferCache.unpin(page);
            }
        }
    }

    /** Decode the neighbor-list field (a length-prefixed byte array) into its raw entry bytes. */
    private static byte[] readNeighborField(ITreeIndexTupleReference frameTuple) throws HyracksDataException {
        if (frameTuple.getFieldCount() <= NEIGHBOR_FIELD_INDEX) {
            return VTreeLeafNeighborList.EMPTY;
        }
        byte[] fieldData = frameTuple.getFieldData(NEIGHBOR_FIELD_INDEX);
        int fieldStart = frameTuple.getFieldStart(NEIGHBOR_FIELD_INDEX);
        int fieldLength = frameTuple.getFieldLength(NEIGHBOR_FIELD_INDEX);
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(fieldData, fieldStart, fieldLength));
        return ByteArraySerializerDeserializer.INSTANCE.deserialize(dis);
    }
}
