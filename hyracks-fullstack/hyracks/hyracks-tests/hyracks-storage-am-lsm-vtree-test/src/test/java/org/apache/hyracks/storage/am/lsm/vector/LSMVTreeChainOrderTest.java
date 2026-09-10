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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIOOperation.LSMIOOperationStatus;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.vector.impls.LSMVTree;
import org.apache.hyracks.storage.am.lsm.vector.util.DiskChainReader;
import org.apache.hyracks.storage.am.lsm.vector.util.DiskChainReader.Chain;
import org.apache.hyracks.storage.am.lsm.vector.util.DiskChainReader.Entry;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVTreeTestContext;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVTreeTestHarness;
import org.apache.hyracks.storage.am.lsm.vector.util.VectorTestStructure;
import org.apache.hyracks.storage.am.vector.AbstractVectorTreeTestContext;
import org.apache.hyracks.storage.am.vector.VectorTreeTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Every writer of a disk component leaves each cluster's data-page chain sorted by {@code <distance,
 * primary key>}, and a key appears at most once per component. The chain is what the LSM merge
 * cursor consumes as a sorted stream, so both hold across page splits, replaced keys and delete markers.
 */
public class LSMVTreeChainOrderTest {

    private final LSMVTreeTestHarness harness = new LSMVTreeTestHarness();
    private final VectorTreeTestUtils testUtils = new VectorTreeTestUtils();

    /** Records per cluster in the bulk-loaded component; the ties are built by insert, not by load. */
    private static final int BULK_RECORDS_PER_CLUSTER = 8;

    /** Every tied record carries this vector, so all of them sit at one distance from their centroid. */
    private static final double[] TIE_VECTOR = { 20.2, 30.0, 20.0 };

    private static final String TIE_PK_PREFIX = "pk_tie_";

    /** Enough same-distance records to split a 512-byte data page at least once at ~30 bytes a tuple. */
    private static final int TIE_RECORDS = 40;

    /** Keys 1..40 have widths of 8 and 9 characters, so their encoded order is not their typed order. */
    private static final int VARIABLE_WIDTH_KEYS = 40;

    /** Inserted last, and lower than every {@link #TIE_PK_PREFIX} key already on the chain. */
    private static final String LOW_PK = TIE_PK_PREFIX + "000";

    /** Enough descending inserts for the first page to split many times while the directory fills. */
    private static final int WIDTH_ALTERNATING_KEYS = 200;
    /** Widens every other key so a lowered separator can outgrow the directory page's free space. */
    private static final String WIDE_SUFFIX = "w".repeat(60);
    /** Six 31-byte records plus one 232-byte record fill a 512-byte page; a second wide record splits it. */
    private static final int NARROW_RECORDS = 6;
    /** Makes a 232-byte record, under the 239-byte bound a 512-byte page admits on the write path. */
    private static final String WIDE_INCLUDE = "g".repeat(205);
    private static final String SHORT_INCLUDE = "s";

    /** Long enough that a replacement carrying it cannot overwrite the original in place. */
    private static final String LONG_INCLUDE = "g".repeat(100);

    @Before
    public void setUp() throws HyracksDataException {
        harness.setUp();
    }

    @After
    public void tearDown() throws HyracksDataException {
        harness.tearDown();
    }

    @Test
    public void tiedDistanceKeepsChainSortedAcrossPages() throws Exception {
        withIndex(newContext(false), (ctx, tree) -> {
            testUtils.insertRecordsIntoMemoryComponent(ctx, List.of(tiedRecords()));
            testUtils.insertRecordsIntoMemoryComponent(ctx, List.of(List.of(tuple(TIE_VECTOR, LOW_PK))));
            flush(ctx);

            List<Chain> chains = DiskChainReader.read(tree);
            assertTrue("tie must span two or more pages", pagesHoldingTiedKeys(chains) >= 2);
            assertFalse("last insert must be on a chain", DiskChainReader.entriesFor(chains, LOW_PK).isEmpty());
            assertEquals(List.of(), DiskChainReader.inversions(chains));
        });
    }

    /** Matter and its marker are reconciled on field 0, so the marker must carry the same bits. */
    @Test
    public void deleteMarkerCarriesTheMattersDistanceBits() throws Exception {
        withIndex(newContext(false), (ctx, tree) -> {
            String pk = TIE_PK_PREFIX + "marker";
            testUtils.insertRecordsIntoMemoryComponent(ctx, List.of(List.of(tuple(TIE_VECTOR, pk))));
            flush(ctx);
            testUtils.deleteRecordsFromIndex(ctx, List.of(tuple(TIE_VECTOR, pk)));
            flush(ctx);

            List<Entry> entries = DiskChainReader.entriesFor(DiskChainReader.read(tree), pk);
            assertEquals("matter and marker, in two components", 2, entries.size());
            assertEquals(entries.get(0).distanceBits(), entries.get(1).distanceBits());
        });
    }

    @Test
    public void reinsertAfterDeleteLeavesOneEntry() throws Exception {
        withIndex(newContext(false), (ctx, tree) -> {
            String pk = TIE_PK_PREFIX + "replace";
            testUtils.insertRecordsIntoMemoryComponent(ctx, List.of(List.of(tuple(TIE_VECTOR, pk))));
            testUtils.deleteRecordsFromIndex(ctx, List.of(tuple(TIE_VECTOR, pk)));
            testUtils.insertRecordsIntoMemoryComponent(ctx, List.of(List.of(tuple(TIE_VECTOR, pk))));
            flush(ctx);

            List<Chain> chains = DiskChainReader.read(tree);
            assertEquals(1, DiskChainReader.entriesFor(chains, pk).size());
            assertEquals(List.of(), DiskChainReader.inversions(chains));
        });
    }

    /** A replacement wider than the entry it replaces must be the entry that survives. */
    @Test
    public void reinsertWithLongerIncludeKeepsTheGrownValue() throws Exception {
        withIndex(newContext(true), (ctx, tree) -> {
            testUtils.insertRecordsIntoMemoryComponent(ctx, List.of(tiedIncludeRecords()));
            String pk = TIE_PK_PREFIX + "grow";
            testUtils.insertRecordsIntoMemoryComponent(ctx, List.of(List.of(tuple(TIE_VECTOR, SHORT_INCLUDE, pk))));
            testUtils.deleteRecordsFromIndex(ctx, List.of(tuple(TIE_VECTOR, SHORT_INCLUDE, pk)));
            testUtils.insertRecordsIntoMemoryComponent(ctx, List.of(List.of(tuple(TIE_VECTOR, LONG_INCLUDE, pk))));
            flush(ctx);

            List<Chain> chains = DiskChainReader.read(tree);
            List<Entry> entries = DiskChainReader.entriesFor(chains, pk);
            assertEquals(1, entries.size());
            assertEquals(LONG_INCLUDE, entries.get(0).include());
            assertEquals(List.of(), DiskChainReader.inversions(chains));
        });
    }

    @Test
    public void everyDiskWriterLeavesTheChainSorted() throws Exception {
        withIndex(newContext(false), (ctx, tree) -> {
            assertChainSorted("after the bulk load", tree);

            testUtils.insertRecordsIntoMemoryComponent(ctx, List.of(tiedRecords()));
            flush(ctx);
            assertChainSorted("after the flush", tree);

            testUtils.insertRecordsIntoMemoryComponent(ctx, List.of(List.of(tuple(TIE_VECTOR, LOW_PK))));
            flush(ctx);
            assertChainSorted("after the second flush", tree);

            merge(ctx, tree);
            assertChainSorted("after the merge", tree);
        });
    }

    /** Keys whose encoded order and typed order disagree, so only the key comparators sort them right. */
    @Test
    public void tiedDistanceOrdersByComparatorNotBytes() throws Exception {
        List<String> keys = new ArrayList<>();
        for (int i = 1; i <= VARIABLE_WIDTH_KEYS; i++) {
            keys.add(TIE_PK_PREFIX + i);
        }
        assertTrue("fixture must order differently by comparator than by bytes", discriminatesByteOrder(keys));

        withIndex(newContext(false), (ctx, tree) -> {
            List<ITupleReference> tied = new ArrayList<>();
            for (String key : keys) {
                tied.add(tuple(TIE_VECTOR, key));
            }
            testUtils.insertRecordsIntoMemoryComponent(ctx, List.of(tied));
            flush(ctx);

            List<Chain> chains = DiskChainReader.read(tree);
            assertTrue("tied inserts must reach a flushed component", pagesHoldingTiedKeys(chains) > 0);
            assertEquals(List.of(), DiskChainReader.inversions(chains));
        });
    }

    /**
     * A split lowers the original page's separator to a key that can be wider than the one it replaces,
     * and a full directory page then has to split to take it. Descending inserts of keys alternating
     * between two widths keep producing such replacements on the first page of the chain until one of
     * them lands on a directory page with no room for it.
     */
    @Test
    public void widerSeparatorOnFullDirectoryKeepsDirectoryInChainOrder() throws Exception {
        withIndex(newContext(false), (ctx, tree) -> {
            for (int i = 1; i <= WIDTH_ALTERNATING_KEYS; i++) {
                String key = TIE_PK_PREFIX + String.format("%03d", i) + "w".repeat((i % 5) * 20);
                testUtils.insertRecordsIntoMemoryComponent(ctx, List.of(List.of(tuple(TIE_VECTOR, key))));
            }
            flush(ctx);
            List<Chain> chains = DiskChainReader.read(tree);
            assertTrue("inserts must reach several pages", pagesHoldingTiedKeys(chains) >= 4);
            assertEquals(List.of(), DiskChainReader.directoryOrderMismatches(tree));
            assertEquals(List.of(), DiskChainReader.inversions(chains));
        });
    }

    /**
     * A re-insert replaces the delete marker on its page even when that page has no room for another
     * tuple. Each round writes one filler, one marker and one replacing matter, all the same width, so
     * the replace lands on a full page every time the marker took the page's last slot.
     */
    @Test
    public void replaceOnFullPageLeavesOneEntry() throws Exception {
        withIndex(newContext(false), (ctx, tree) -> {
            List<ITupleReference> onDisk = tiedRecords();
            testUtils.insertRecordsIntoMemoryComponent(ctx, List.of(onDisk));
            flush(ctx);
            for (int i = 0; i < onDisk.size(); i++) {
                testUtils.insertRecordsIntoMemoryComponent(ctx,
                        List.of(List.of(tuple(TIE_VECTOR, TIE_PK_PREFIX + "filler" + i))));
                testUtils.deleteRecordsFromIndex(ctx, List.of(onDisk.get(i)));
                testUtils.insertRecordsIntoMemoryComponent(ctx, List.of(List.of(onDisk.get(i))));
            }
            flush(ctx);
            List<Chain> chains = DiskChainReader.read(tree);
            assertEquals(List.of(), DiskChainReader.duplicateKeys(chains));
            assertEquals(List.of(), DiskChainReader.inversions(chains));
        });
    }

    /**
     * A split must leave room for the tuple it places. Six narrow records and one wide one fill the page,
     * and a second wide record sorts after the first, so a split by tuple count would route it to the
     * half that already holds the first wide record and has no room left for it.
     */
    @Test
    public void splitLeavesRoomForAWideTupleOnTheHeavySide() throws Exception {
        withIndex(newContext(true), (ctx, tree) -> {
            for (int i = 1; i <= NARROW_RECORDS; i++) {
                testUtils.insertRecordsIntoMemoryComponent(ctx,
                        List.of(List.of(tuple(TIE_VECTOR, SHORT_INCLUDE, TIE_PK_PREFIX + "a" + i))));
            }
            testUtils.insertRecordsIntoMemoryComponent(ctx,
                    List.of(List.of(tuple(TIE_VECTOR, WIDE_INCLUDE, TIE_PK_PREFIX + "b"))));
            testUtils.insertRecordsIntoMemoryComponent(ctx,
                    List.of(List.of(tuple(TIE_VECTOR, WIDE_INCLUDE, TIE_PK_PREFIX + "c"))));
            flush(ctx);

            List<Chain> chains = DiskChainReader.read(tree);
            for (int i = 1; i <= NARROW_RECORDS; i++) {
                assertEquals(1, DiskChainReader.entriesFor(chains, TIE_PK_PREFIX + "a" + i).size());
            }
            for (String wide : List.of(TIE_PK_PREFIX + "b", TIE_PK_PREFIX + "c")) {
                List<Entry> entries = DiskChainReader.entriesFor(chains, wide);
                assertEquals(1, entries.size());
                assertEquals(WIDE_INCLUDE, entries.get(0).include());
            }
            assertEquals(List.of(), DiskChainReader.duplicateKeys(chains));
            assertEquals(List.of(), DiskChainReader.inversions(chains));
        });
    }

    /** Whether {@code keys} order differently by the comparator than by length and then bytes. */
    private static boolean discriminatesByteOrder(List<String> keys) {
        List<String> byComparator = new ArrayList<>(keys);
        byComparator.sort(String::compareTo);
        List<String> byEncodedBytes = new ArrayList<>(keys);
        byEncodedBytes.sort((left, right) -> left.length() != right.length() ? left.length() - right.length()
                : left.compareTo(right));
        return !byComparator.equals(byEncodedBytes);
    }

    private static int pagesHoldingTiedKeys(List<Chain> chains) {
        Set<Integer> pages = new HashSet<>();
        for (Entry entry : DiskChainReader.entries(chains)) {
            if (entry.primaryKey().startsWith(TIE_PK_PREFIX)) {
                pages.add(entry.pageId());
            }
        }
        return pages.size();
    }

    private static void assertChainSorted(String when, LSMVTree tree) throws HyracksDataException {
        assertEquals("chain order " + when, List.of(), DiskChainReader.inversions(DiskChainReader.read(tree)));
    }

    private interface IndexBody {
        void run(AbstractVectorTreeTestContext ctx, LSMVTree tree) throws Exception;
    }

    /** Creates and activates the index, bulk-loads the static structure, runs {@code body}, deactivates. */
    private void withIndex(AbstractVectorTreeTestContext ctx, IndexBody body) throws Exception {
        try {
            ctx.getIndex().create();
            ctx.getIndex().activate();
            testUtils.buildStaticStructure(ctx);
            body.run(ctx, (LSMVTree) ctx.getIndex());
        } finally {
            ctx.getIndex().deactivate();
        }
    }

    /** Ascending keys above {@link #LOW_PK}, all at one distance. */
    private List<ITupleReference> tiedRecords() throws HyracksDataException {
        List<ITupleReference> tied = new ArrayList<>();
        for (int i = 0; i < TIE_RECORDS; i++) {
            tied.add(tuple(TIE_VECTOR, TIE_PK_PREFIX + (100 + i)));
        }
        return tied;
    }

    private List<ITupleReference> tiedIncludeRecords() throws HyracksDataException {
        List<ITupleReference> tied = new ArrayList<>();
        for (int i = 0; i < TIE_RECORDS; i++) {
            tied.add(tuple(TIE_VECTOR, SHORT_INCLUDE, TIE_PK_PREFIX + (100 + i)));
        }
        return tied;
    }

    private void flush(AbstractVectorTreeTestContext ctx) throws HyracksDataException, InterruptedException {
        ILSMIndexAccessor accessor =
                (ILSMIndexAccessor) ctx.getIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);
        await(accessor.scheduleFlush());
    }

    private void merge(AbstractVectorTreeTestContext ctx, LSMVTree tree)
            throws HyracksDataException, InterruptedException {
        ILSMIndexAccessor accessor =
                (ILSMIndexAccessor) ctx.getIndex().createAccessor(NoOpIndexAccessParameters.INSTANCE);
        await(accessor.scheduleMerge(tree.getDiskComponents()));
    }

    private static void await(ILSMIOOperation op) throws HyracksDataException, InterruptedException {
        op.sync();
        if (op.getStatus() == LSMIOOperationStatus.FAILURE) {
            throw HyracksDataException.create(op.getFailure());
        }
    }

    /** A context over the three-level fixture, with one variable-width INCLUDE field when asked. */
    private AbstractVectorTreeTestContext newContext(boolean withInclude) throws Exception {
        ISerializerDeserializer[] includeSerdes = { new UTF8StringSerializerDeserializer() };
        VectorTestStructure struct = withInclude
                ? VectorTestStructure.threeDim3Level().withIncludeFields(includeSerdes,
                        (centroidId, recordIndex) -> new Object[] { SHORT_INCLUDE })
                : VectorTestStructure.threeDim3Level();
        VectorTestStructure.BulkLoadRecordFormat format =
                withInclude ? VectorTestStructure.BulkLoadRecordFormat.NAIVE_WITH_INCLUDES
                        : VectorTestStructure.BulkLoadRecordFormat.NAIVE;
        AbstractVectorTreeTestContext ctx = withInclude
                ? LSMVTreeTestContext.create(harness.getNcConfig(), harness.getIOManager(),
                        harness.getVirtualBufferCaches(), harness.getFileReference(), harness.getDiskBufferCache(),
                        struct.getDataRecordSerdes(format), struct.getVectorDimension(), harness.getMergePolicy(),
                        harness.getOperationTracker(), harness.getIOScheduler(),
                        harness.getIOOperationCallbackFactory(), harness.getPageWriteCallbackFactory(),
                        harness.getMetadataPageManagerFactory(), includeSerdes.length)
                : LSMVTreeTestContext.create(harness.getNcConfig(), harness.getIOManager(),
                        harness.getVirtualBufferCaches(), harness.getFileReference(), harness.getDiskBufferCache(),
                        struct.getDataRecordSerdes(format), struct.getVectorDimension(), harness.getMergePolicy(),
                        harness.getOperationTracker(), harness.getIOScheduler(),
                        harness.getIOOperationCallbackFactory(), harness.getPageWriteCallbackFactory(),
                        harness.getMetadataPageManagerFactory());
        ctx.setStaticStructureCentroids(struct.buildCentroidTuples());
        ctx.setNumClustersPerLevel(struct.getNumClustersPerLevel());
        ctx.setNumCentroidsPerLevel(struct.getCentroidsPerCluster());
        ctx.setDataRecords(struct.generateBulkLoadRecords(format, BULK_RECORDS_PER_CLUSTER));
        return ctx;
    }

    /** An input tuple: the vector, then the string fields in the input layout's order. */
    private static ITupleReference tuple(double[] vector, String... fields) throws HyracksDataException {
        try {
            ArrayTupleBuilder builder = new ArrayTupleBuilder(1 + fields.length);
            DoubleArraySerializerDeserializer.INSTANCE.serialize(vector, builder.getDataOutput());
            builder.addFieldEndOffset();
            UTF8StringSerializerDeserializer strings = new UTF8StringSerializerDeserializer();
            for (String field : fields) {
                strings.serialize(field, builder.getDataOutput());
                builder.addFieldEndOffset();
            }
            ArrayTupleReference ref = new ArrayTupleReference();
            ref.reset(builder.getFieldEndOffsets(), builder.getByteArray());
            return ref;
        } catch (Exception e) {
            throw HyracksDataException.create(e);
        }
    }
}
