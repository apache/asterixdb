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
package org.apache.hyracks.storage.am.lsm.vector.multithread;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleArraySerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.TestOperationCallback;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVTreeTestContext;
import org.apache.hyracks.storage.am.lsm.vector.util.LSMVTreeTestHarness;
import org.apache.hyracks.storage.am.lsm.vector.util.VectorTestStructure;
import org.apache.hyracks.storage.am.lsm.vector.util.VectorTestStructure.BulkLoadRecordFormat;
import org.apache.hyracks.storage.am.vector.AbstractVectorTreeTestContext;
import org.apache.hyracks.storage.am.vector.TestDoubleArrayVectorAccessor;
import org.apache.hyracks.storage.am.vector.VectorTreeTestUtils;
import org.apache.hyracks.storage.am.vector.api.IVTreeBinaryAccessorFactory;
import org.apache.hyracks.storage.am.vector.impls.VTreeSearchPredicate;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Multi-threaded test for LSMVTree.
 *
 * All insert threads target vectors near a single leaf centroid (c10 at [20, 30, 20])
 * to maximize contention on the same cluster's data pages in the memory component.
 *
 * Verification strategy:
 * - After concurrent inserts: search near c10, collect all PKs, assert every
 *   inserted PK is found and centroid_id == 10 for inserted records.
 */
public class LSMVTreeMultiThreadTest {

    private static final Logger LOGGER = LogManager.getLogger();

    private static final VectorTestStructure DATASET = VectorTestStructure.threeDim3Level();

    // Target cluster: c10 at [20, 30, 20] (first leaf centroid, index 0)
    private static final int TARGET_CENTROID_INDEX = 0;
    private static final double[] TARGET_CENTROID = DATASET.getLeafCentroids()[TARGET_CENTROID_INDEX];
    private static final int TARGET_CENTROID_ID = DATASET.getFirstLeafCentroidId() + TARGET_CENTROID_INDEX;
    private static final int BULK_LOADED_PER_CENTROID = 100;

    private static final int RECORDS_PER_THREAD = 50;

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

    /**
     * Single-threaded insert test to isolate metadata split bugs from concurrency issues.
     * Uses 1 thread with enough records to trigger metadata page overflow.
     */
    @Test
    public void testSingleThreadManyInserts() throws Exception {
        LOGGER.info("testSingleThreadManyInserts: 1 thread with 400 records");
        runConcurrentInsertTest(1, 400);
    }

    @Test
    public void testConcurrentInserts() throws Exception {
        int numThreads = Math.max(4, Runtime.getRuntime().availableProcessors());
        LOGGER.info("testConcurrentInserts: using {} threads", numThreads);
        runConcurrentInsertTest(numThreads);
    }

    /**
     * Test concurrent inserts with higher thread count for stress testing.
     */
    @Test
    public void testConcurrentInsertsHighContention() throws Exception {
        int numThreads = Math.max(8, Runtime.getRuntime().availableProcessors() * 4);
        LOGGER.info("testConcurrentInsertsHighContention: using {} threads", numThreads);
        runConcurrentInsertTest(numThreads);
    }

    /**
     * Test concurrent inserts and searches on cluster c10.
     * Insert threads write to c10 while search threads read from c10 simultaneously.
     * Search threads assert result count >= bulk-loaded floor (100).
     * After completion, verify all inserted PKs are present.
     */
    @Test
    public void testConcurrentInsertsAndSearches() throws Exception {
        int numInsertThreads = Math.max(2, Runtime.getRuntime().availableProcessors() / 2);
        int numSearchThreads = Math.max(2, Runtime.getRuntime().availableProcessors() / 2);
        LOGGER.info("testConcurrentInsertsAndSearches: {} insert threads, {} search threads", numInsertThreads,
                numSearchThreads);

        AbstractVectorTreeTestContext ctx = setupIndex();

        try {
            // Build expected PK set for all insert threads
            Set<String> expectedInsertedPKs = new HashSet<>();
            List<List<ITupleReference>> threadTuples = new ArrayList<>();
            for (int t = 0; t < numInsertThreads; t++) {
                List<ITupleReference> tuples = DATASET.generateInsertRecordsNearCentroid(TARGET_CENTROID_INDEX,
                        RECORDS_PER_THREAD, "mt_t" + t + "_", t * 1000L);
                threadTuples.add(tuples);
                for (int i = 0; i < RECORDS_PER_THREAD; i++) {
                    expectedInsertedPKs.add("mt_t" + t + "_" + i);
                }
            }

            ExecutorService executor = Executors.newFixedThreadPool(numInsertThreads + numSearchThreads);

            // Submit insert workers
            List<LSMVTreeTestWorker> insertWorkers = new ArrayList<>();
            for (int t = 0; t < numInsertThreads; t++) {
                IIndexAccessor accessor = ctx.getIndex().createAccessor(
                        new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE));
                LSMVTreeTestWorker worker = new LSMVTreeTestWorker(accessor, threadTuples.get(t));
                insertWorkers.add(worker);
                executor.submit(worker);
            }

            // Submit search workers — search c10 concurrently with inserts
            List<SearchWorker> searchWorkers = new ArrayList<>();
            for (int t = 0; t < numSearchThreads; t++) {
                IndexAccessParameters iap =
                        new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);
                iap.getParameters().put(IVTreeBinaryAccessorFactory.IAP_KEY,
                        TestDoubleArrayVectorAccessor.Factory.INSTANCE);
                IIndexAccessor accessor = ctx.getIndex().createAccessor(iap);
                SearchWorker worker = new SearchWorker(accessor, TARGET_CENTROID, BULK_LOADED_PER_CENTROID);
                searchWorkers.add(worker);
                executor.submit(worker);
            }

            executor.shutdown();
            assertTrue("Threads did not finish in time", executor.awaitTermination(60, TimeUnit.SECONDS));

            // Check for exceptions in all workers
            for (LSMVTreeTestWorker worker : insertWorkers) {
                if (worker.getException() != null) {
                    fail("Insert worker failed: " + worker.getException().getMessage());
                }
            }
            for (SearchWorker worker : searchWorkers) {
                if (worker.getException() != null) {
                    fail("Search worker failed: " + worker.getException().getMessage());
                }
            }

            // Post-verification: all inserted PKs retrievable
            verifyInsertedRecords(ctx, expectedInsertedPKs);

            ctx.getIndex().validate();
            LOGGER.info("testConcurrentInsertsAndSearches: passed");

        } finally {
            ctx.getIndex().deactivate();
            ctx.getIndex().destroy();
        }
    }

    /**
     * Concurrent inserts + deletes on the same cluster c10. Exercises the delete path's
     * directory-chain traversal (tryPhysicalDelete) alongside the insert path's, both funneled at one
     * cluster to build a multi-page directory chain. Delete threads remove a pre-inserted, disjoint
     * "del_" set; insert threads add a fresh "mt_" set. After completion: every mt_ PK is present, no
     * del_ PK survives, and the bulk-loaded records are intact.
     */
    @Test
    public void testConcurrentInsertsAndDeletes() throws Exception {
        int numInsertThreads = Math.max(2, Runtime.getRuntime().availableProcessors() / 2);
        int numDeleteThreads = Math.max(2, Runtime.getRuntime().availableProcessors() / 2);
        int deletePerThread = 60;
        LOGGER.info("testConcurrentInsertsAndDeletes: {} insert threads, {} delete threads", numInsertThreads,
                numDeleteThreads);

        AbstractVectorTreeTestContext ctx = setupIndex();

        try {
            // Pre-insert the to-be-deleted set single-threaded (one disjoint "del_" block per delete thread).
            List<List<ITupleReference>> deleteTuples = new ArrayList<>();
            for (int t = 0; t < numDeleteThreads; t++) {
                List<ITupleReference> block = DATASET.generateInsertRecordsNearCentroid(TARGET_CENTROID_INDEX,
                        deletePerThread, "del_t" + t + "_", 500000L + t * 1000L);
                deleteTuples.add(block);
                IIndexAccessor pre = ctx.getIndex().createAccessor(
                        new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE));
                for (ITupleReference tuple : block) {
                    pre.insert(tuple);
                }
            }

            // Fresh, disjoint "mt_" set inserted concurrently with the deletes.
            Set<String> expectedInsertedPKs = new HashSet<>();
            List<List<ITupleReference>> insertTuples = new ArrayList<>();
            for (int t = 0; t < numInsertThreads; t++) {
                List<ITupleReference> tuples = DATASET.generateInsertRecordsNearCentroid(TARGET_CENTROID_INDEX,
                        RECORDS_PER_THREAD, "mt_t" + t + "_", t * 1000L);
                insertTuples.add(tuples);
                for (int i = 0; i < RECORDS_PER_THREAD; i++) {
                    expectedInsertedPKs.add("mt_t" + t + "_" + i);
                }
            }

            ExecutorService executor = Executors.newFixedThreadPool(numInsertThreads + numDeleteThreads);

            List<LSMVTreeTestWorker> insertWorkers = new ArrayList<>();
            for (int t = 0; t < numInsertThreads; t++) {
                IIndexAccessor accessor = ctx.getIndex().createAccessor(
                        new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE));
                LSMVTreeTestWorker worker = new LSMVTreeTestWorker(accessor, insertTuples.get(t));
                insertWorkers.add(worker);
                executor.submit(worker);
            }

            List<DeleteWorker> deleteWorkers = new ArrayList<>();
            for (int t = 0; t < numDeleteThreads; t++) {
                IIndexAccessor accessor = ctx.getIndex().createAccessor(
                        new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE));
                DeleteWorker worker = new DeleteWorker(accessor, deleteTuples.get(t));
                deleteWorkers.add(worker);
                executor.submit(worker);
            }

            executor.shutdown();
            assertTrue("Threads did not finish in time", executor.awaitTermination(60, TimeUnit.SECONDS));

            for (LSMVTreeTestWorker worker : insertWorkers) {
                if (worker.getException() != null) {
                    fail("Insert worker failed: " + worker.getException().getMessage());
                }
            }
            for (DeleteWorker worker : deleteWorkers) {
                if (worker.getException() != null) {
                    fail("Delete worker failed: " + worker.getException().getMessage());
                }
            }

            verifyInsertedAndDeleted(ctx, expectedInsertedPKs, "del_");

            ctx.getIndex().validate();
            LOGGER.info("testConcurrentInsertsAndDeletes: passed");

        } finally {
            ctx.getIndex().deactivate();
            ctx.getIndex().destroy();
        }
    }

    private void runConcurrentInsertTest(int numThreads) throws Exception {
        runConcurrentInsertTest(numThreads, RECORDS_PER_THREAD);
    }

    private void runConcurrentInsertTest(int numThreads, int recordsPerThread) throws Exception {
        AbstractVectorTreeTestContext ctx = setupIndex();

        try {
            // Build expected PK set and generate tuples
            Set<String> expectedInsertedPKs = new HashSet<>();
            List<List<ITupleReference>> threadTuples = new ArrayList<>();
            for (int t = 0; t < numThreads; t++) {
                List<ITupleReference> tuples = DATASET.generateInsertRecordsNearCentroid(TARGET_CENTROID_INDEX,
                        recordsPerThread, "mt_t" + t + "_", t * 1000L);
                threadTuples.add(tuples);
                for (int i = 0; i < recordsPerThread; i++) {
                    expectedInsertedPKs.add("mt_t" + t + "_" + i);
                }
            }

            // Create one accessor per thread (each gets its own OpContext)
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            List<LSMVTreeTestWorker> workers = new ArrayList<>();

            for (int t = 0; t < numThreads; t++) {
                IIndexAccessor accessor = ctx.getIndex().createAccessor(
                        new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE));
                LSMVTreeTestWorker worker = new LSMVTreeTestWorker(accessor, threadTuples.get(t));
                workers.add(worker);
                executor.submit(worker);
            }

            executor.shutdown();
            assertTrue("Threads did not finish in time", executor.awaitTermination(60, TimeUnit.SECONDS));

            // Check no exceptions were thrown
            for (int t = 0; t < numThreads; t++) {
                LSMVTreeTestWorker worker = workers.get(t);
                if (worker.getException() != null) {
                    worker.getException().printStackTrace();
                    fail("Thread " + t + " failed with: " + worker.getException().getMessage());
                }
            }

            // Verify all inserted records are retrievable
            verifyInsertedRecords(ctx, expectedInsertedPKs);

            ctx.getIndex().validate();
            LOGGER.info("Concurrent insert test with {} threads: all {} inserted PKs verified", numThreads,
                    expectedInsertedPKs.size());

        } finally {
            ctx.getIndex().deactivate();
            ctx.getIndex().destroy();
        }
    }

    /**
     * Search near c10 with nprobe=MAX and k=MAX to scan all clusters across
     * both memory and disk components. Verify:
     * 1. Every expected inserted PK is found
     * 2. All inserted records have centroid_id == TARGET_CENTROID_ID
     * 3. Bulk-loaded PKs for c10 are still present
     */
    private void verifyInsertedRecords(AbstractVectorTreeTestContext ctx, Set<String> expectedInsertedPKs)
            throws Exception {
        // Create query tuple for c10
        ArrayTupleBuilder queryTupleBuilder = new ArrayTupleBuilder(1);
        queryTupleBuilder.addField(DoubleArraySerializerDeserializer.INSTANCE, TARGET_CENTROID);
        ArrayTupleReference queryTuple = new ArrayTupleReference();
        queryTuple.reset(queryTupleBuilder.getFieldEndOffsets(), queryTupleBuilder.getByteArray());

        // Search with nprobe=MAX, k=MAX to get all records from all clusters
        VTreeSearchPredicate predicate = new VTreeSearchPredicate();
        predicate.setMinProbeFraction(0.4);
        predicate.setQueryTuple(queryTuple);
        predicate.setQueryFieldIndex(0);
        predicate.setK(Integer.MAX_VALUE);
        predicate.setEpsilon(0.0);

        IndexAccessParameters iap =
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);
        iap.getParameters().put(IVTreeBinaryAccessorFactory.IAP_KEY, TestDoubleArrayVectorAccessor.Factory.INSTANCE);

        IIndexAccessor accessor = ctx.getIndex().createAccessor(iap);
        IIndexCursor cursor = accessor.createSearchCursor(false);

        Set<String> foundInsertedPKs = new HashSet<>();
        Set<String> foundBulkLoadedPKs = new HashSet<>();
        int totalResults = 0;

        try {
            accessor.search(cursor, predicate);

            while (cursor.hasNext()) {
                cursor.next();
                ITupleReference tuple = cursor.getTuple();
                totalResults++;

                // Result tuple format: <distance, centroid_id, primary_key>
                Object[] values =
                        TupleUtils.deserializeTuple(tuple,
                                new ISerializerDeserializer[] { DoubleSerializerDeserializer.INSTANCE,
                                        IntegerSerializerDeserializer.INSTANCE,
                                        new UTF8StringSerializerDeserializer() });
                int centroidId = (Integer) values[1];
                String pk = (String) values[2];

                if (pk.startsWith("mt_")) {
                    foundInsertedPKs.add(pk);
                    assertEquals("Inserted record should be assigned to target centroid c" + TARGET_CENTROID_ID,
                            TARGET_CENTROID_ID, centroidId);
                } else if (pk.startsWith("pk_c_" + TARGET_CENTROID_ID + "_")) {
                    foundBulkLoadedPKs.add(pk);
                }
            }
        } finally {
            cursor.close();
            cursor.destroy();
        }

        LOGGER.info("Verification: {} total results, {} inserted PKs found, {} bulk-loaded PKs found", totalResults,
                foundInsertedPKs.size(), foundBulkLoadedPKs.size());

        // 1. Every inserted PK must be found (no lost writes)
        Set<String> missingPKs = new HashSet<>(expectedInsertedPKs);
        missingPKs.removeAll(foundInsertedPKs);
        assertTrue("Missing " + missingPKs.size() + " inserted PKs: " + firstN(missingPKs, 10), missingPKs.isEmpty());

        // 2. Count must match exactly
        assertEquals("Inserted PK count mismatch", expectedInsertedPKs.size(), foundInsertedPKs.size());

        // 3. Bulk-loaded records for c10 must still be present
        assertEquals("Bulk-loaded records for c10 should be intact", BULK_LOADED_PER_CENTROID,
                foundBulkLoadedPKs.size());
    }

    /**
     * Scan near c10 and assert: every expected inserted PK is present, NO primary key with
     * {@code deletedPrefix} survives (all concurrent deletes took effect), and the bulk-loaded records
     * for c10 are intact.
     */
    private void verifyInsertedAndDeleted(AbstractVectorTreeTestContext ctx, Set<String> expectedInsertedPKs,
            String deletedPrefix) throws Exception {
        ArrayTupleBuilder queryTupleBuilder = new ArrayTupleBuilder(1);
        queryTupleBuilder.addField(DoubleArraySerializerDeserializer.INSTANCE, TARGET_CENTROID);
        ArrayTupleReference queryTuple = new ArrayTupleReference();
        queryTuple.reset(queryTupleBuilder.getFieldEndOffsets(), queryTupleBuilder.getByteArray());

        VTreeSearchPredicate predicate = new VTreeSearchPredicate();
        predicate.setMinProbeFraction(0.4);
        predicate.setQueryTuple(queryTuple);
        predicate.setQueryFieldIndex(0);
        predicate.setK(Integer.MAX_VALUE);
        predicate.setEpsilon(0.0);

        IndexAccessParameters iap =
                new IndexAccessParameters(TestOperationCallback.INSTANCE, TestOperationCallback.INSTANCE);
        iap.getParameters().put(IVTreeBinaryAccessorFactory.IAP_KEY, TestDoubleArrayVectorAccessor.Factory.INSTANCE);

        IIndexAccessor accessor = ctx.getIndex().createAccessor(iap);
        IIndexCursor cursor = accessor.createSearchCursor(false);

        Set<String> foundInsertedPKs = new HashSet<>();
        Set<String> foundDeletedPKs = new HashSet<>();
        int foundBulkLoaded = 0;

        try {
            accessor.search(cursor, predicate);
            while (cursor.hasNext()) {
                cursor.next();
                ITupleReference tuple = cursor.getTuple();
                Object[] values =
                        TupleUtils.deserializeTuple(tuple,
                                new ISerializerDeserializer[] { DoubleSerializerDeserializer.INSTANCE,
                                        IntegerSerializerDeserializer.INSTANCE,
                                        new UTF8StringSerializerDeserializer() });
                String pk = (String) values[2];
                if (pk.startsWith("mt_")) {
                    foundInsertedPKs.add(pk);
                } else if (pk.startsWith(deletedPrefix)) {
                    foundDeletedPKs.add(pk);
                } else if (pk.startsWith("pk_c_" + TARGET_CENTROID_ID + "_")) {
                    foundBulkLoaded++;
                }
            }
        } finally {
            cursor.close();
            cursor.destroy();
        }

        Set<String> missingPKs = new HashSet<>(expectedInsertedPKs);
        missingPKs.removeAll(foundInsertedPKs);
        assertTrue("Missing " + missingPKs.size() + " inserted PKs: " + firstN(missingPKs, 10), missingPKs.isEmpty());
        assertTrue("Deleted PKs still present: " + firstN(foundDeletedPKs, 10), foundDeletedPKs.isEmpty());
        assertEquals("Bulk-loaded records for c10 should be intact", BULK_LOADED_PER_CENTROID, foundBulkLoaded);
    }

    private static String firstN(Set<String> set, int n) {
        StringBuilder sb = new StringBuilder("[");
        int count = 0;
        for (String s : set) {
            if (count > 0)
                sb.append(", ");
            sb.append(s);
            if (++count >= n) {
                sb.append(", ...");
                break;
            }
        }
        sb.append("]");
        return sb.toString();
    }

    // ===== Index Setup =====

    private AbstractVectorTreeTestContext setupIndex() throws Exception {
        AbstractVectorTreeTestContext ctx = LSMVTreeTestContext.create(harness.getNcConfig(), harness.getIOManager(),
                harness.getVirtualBufferCaches(), harness.getFileReference(), harness.getDiskBufferCache(),
                DATASET.getDataRecordSerdes(BulkLoadRecordFormat.NAIVE), DATASET.getVectorDimension(),
                harness.getMergePolicy(), harness.getOperationTracker(), harness.getIOScheduler(),
                harness.getIOOperationCallbackFactory(), harness.getPageWriteCallbackFactory(),
                harness.getMetadataPageManagerFactory());

        ctx.setStaticStructureCentroids(DATASET.buildCentroidTuples());
        ctx.setNumClustersPerLevel(DATASET.getNumClustersPerLevel());
        ctx.setNumCentroidsPerLevel(DATASET.getCentroidsPerCluster());
        ctx.setDataRecords(DATASET.generateBulkLoadRecords(BulkLoadRecordFormat.NAIVE, BULK_LOADED_PER_CENTROID));

        ctx.getIndex().create();
        ctx.getIndex().activate();

        testUtils.buildStaticStructure(ctx);
        testUtils.bulkLoadRecords(ctx);

        return ctx;
    }

    // ===== Concurrent Search Worker =====

    /**
     * Worker that searches near c10 concurrently with insert threads.
     * Asserts that each search returns at least {@code minExpectedCount} results
     * (the bulk-loaded records in the disk component are always visible).
     */
    private static class SearchWorker implements Runnable {
        private final IIndexAccessor accessor;
        private final double[] queryVector;
        private final int minExpectedCount;
        private volatile Exception exception;

        SearchWorker(IIndexAccessor accessor, double[] queryVector, int minExpectedCount) {
            this.accessor = accessor;
            this.queryVector = queryVector;
            this.minExpectedCount = minExpectedCount;
        }

        @Override
        public void run() {
            try {
                for (int iter = 0; iter < 5; iter++) {
                    ArrayTupleBuilder queryTupleBuilder = new ArrayTupleBuilder(1);
                    queryTupleBuilder.addField(DoubleArraySerializerDeserializer.INSTANCE, queryVector);
                    ArrayTupleReference queryTuple = new ArrayTupleReference();
                    queryTuple.reset(queryTupleBuilder.getFieldEndOffsets(), queryTupleBuilder.getByteArray());

                    VTreeSearchPredicate predicate = new VTreeSearchPredicate();
                    predicate.setMinProbeFraction(0.4);
                    predicate.setQueryTuple(queryTuple);
                    predicate.setQueryFieldIndex(0);
                    predicate.setK(Integer.MAX_VALUE);
                    predicate.setEpsilon(0.0);

                    IIndexCursor cursor = accessor.createSearchCursor(false);
                    int resultCount = 0;
                    try {
                        accessor.search(cursor, predicate);
                        while (cursor.hasNext()) {
                            cursor.next();
                            cursor.getTuple();
                            resultCount++;
                        }
                    } finally {
                        cursor.close();
                        cursor.destroy();
                    }

                    if (resultCount < minExpectedCount) {
                        throw new AssertionError("Search iteration " + iter + " returned " + resultCount
                                + " results, expected at least " + minExpectedCount);
                    }

                    Thread.sleep(10);
                }
            } catch (Exception e) {
                this.exception = e;
            }
        }

        public Exception getException() {
            return exception;
        }
    }

    // ===== Concurrent Delete Worker =====

    /**
     * Worker that deletes pre-inserted tuples. Holds its own accessor (own OpContext + frames), so it
     * exercises the delete-path directory-chain traversal concurrently with the insert workers.
     */
    private static class DeleteWorker implements Runnable {
        private final IIndexAccessor accessor;
        private final List<ITupleReference> tuples;
        private volatile Exception exception;

        DeleteWorker(IIndexAccessor accessor, List<ITupleReference> tuples) {
            this.accessor = accessor;
            this.tuples = tuples;
        }

        @Override
        public void run() {
            try {
                for (ITupleReference tuple : tuples) {
                    accessor.delete(tuple);
                }
            } catch (Exception e) {
                this.exception = e;
            }
        }

        public Exception getException() {
            return exception;
        }
    }
}
