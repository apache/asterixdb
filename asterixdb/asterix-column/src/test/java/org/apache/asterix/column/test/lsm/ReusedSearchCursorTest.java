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
package org.apache.asterix.column.test.lsm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.column.ColumnManagerFactory;
import org.apache.asterix.column.filter.NoOpColumnFilterEvaluatorFactory;
import org.apache.asterix.column.operation.query.QueryColumnTupleProjector;
import org.apache.asterix.common.config.StorageProperties;
import org.apache.asterix.common.exceptions.NoOpWarningCollector;
import org.apache.asterix.external.api.IStreamDataParser;
import org.apache.asterix.external.parser.ADMDataParser;
import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.CleanJSONPrinterFactoryProvider;
import org.apache.asterix.formats.nontagged.NullIntrospector;
import org.apache.asterix.formats.nontagged.TypeTraitProvider;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.hyracks.api.comm.FixedSizeFrame;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import org.apache.hyracks.storage.am.btree.impls.BatchPredicate;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.ColumnProjectorType;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm.LSMColumnBTreeRangeSearchCursor;
import org.apache.hyracks.storage.am.lsm.btree.column.utils.LSMColumnBTreeUtil;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeTestHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.impls.VirtualBufferCache;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.BufferCache;
import org.apache.hyracks.storage.common.buffercache.ClockPageReplacementStrategy;
import org.apache.hyracks.storage.common.buffercache.ColumnBufferPool;
import org.apache.hyracks.storage.common.buffercache.DefaultDiskCachedPageAllocator;
import org.apache.hyracks.storage.common.buffercache.DelayPageCleanerPolicy;
import org.apache.hyracks.storage.common.buffercache.HeapBufferAllocator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.context.read.DefaultBufferCacheReadContextProvider;
import org.apache.hyracks.storage.common.compression.NoOpCompressorDecompressorFactory;
import org.apache.hyracks.storage.common.disk.NoOpDiskCacheMonitoringService;
import org.apache.hyracks.storage.common.file.FileMapManager;
import org.apache.hyracks.util.trace.ITracer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * An operator keeps its search cursor across the searches it makes -- an index lookup searches once per input tuple --
 * and the LSM cursor keeps each component's accessor while the component count holds. Flushes and merges between two
 * searches can replace every component without changing the count.
 */
public class ReusedSearchCursorTest {
    private static final int PAGE_SIZE = 32 * 1024;
    private static final ARecordType KEYED_TYPE =
            new ARecordType("TestType", new String[] { "id" }, new IAType[] { BuiltinType.AINT64 }, true);
    private static final ARecordType ID_AND_B =
            new ARecordType("r", new String[] { "id", "b" }, new IAType[] { BuiltinType.ANY, BuiltinType.ANY }, true);

    private static final IBinaryComparatorFactory[] CMP_FACTORIES = new IBinaryComparatorFactory[] {
            BinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(BuiltinType.AINT64, true) };

    private final LSMBTreeTestHarness harness = new LSMBTreeTestHarness();
    private IBufferCache diskBufferCache;
    private ColumnBufferPool columnBufferPool;
    private LSMBTree lsmBtree;
    private ILSMIndexAccessor accessor;

    @Before
    public void setUp() throws Exception {
        harness.setUp();
        IIOManager ioManager = harness.getIOManager();
        diskBufferCache = new BufferCache(ioManager,
                new ClockPageReplacementStrategy(new HeapBufferAllocator(), DefaultDiskCachedPageAllocator.INSTANCE,
                        PAGE_SIZE, 4096),
                new DelayPageCleanerPolicy(1000), new FileMapManager(), Integer.MAX_VALUE, 10, Thread::new,
                new HashMap<>(), DefaultBufferCacheReadContextProvider.DEFAULT);
        columnBufferPool = new ColumnBufferPool(PAGE_SIZE, 128, 128L * 1024 * 1024, TimeUnit.MINUTES.toMillis(2));
        List<IVirtualBufferCache> virtualBufferCaches = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            virtualBufferCaches.add(new VirtualBufferCache(new HeapBufferAllocator(), PAGE_SIZE, 1024));
        }
        ITypeTraits[] typeTraits = new ITypeTraits[] { TypeTraitProvider.INSTANCE.getTypeTrait(BuiltinType.AINT64),
                TypeTraitProvider.INSTANCE.getTypeTrait(KEYED_TYPE) };
        // A node registers the storage options with its configuration; this harness's own does not.
        harness.getNCConfig().getConfigManager().register(StorageProperties.Option.values());
        lsmBtree = LSMColumnBTreeUtil.createLSMTree(harness.getNCConfig(), ioManager, virtualBufferCaches,
                harness.getFileReference(), diskBufferCache, columnBufferPool, typeTraits, CMP_FACTORIES,
                new int[] { 0 }, 0.01, harness.getMergePolicy(), harness.getOperationTracker(),
                harness.getIOScheduler(), harness.getIOOperationCallbackFactory(),
                harness.getPageWriteCallbackFactory(), null, harness.getMetadataPageManagerFactory(), false,
                ITracer.NONE, NoOpCompressorDecompressorFactory.INSTANCE,
                TypeTraitProvider.INSTANCE.getTypeTrait(BuiltinType.ANULL),
                NullIntrospector.INSTANCE, new ColumnManagerFactory(KEYED_TYPE, null, List.of(List.of("id")),
                        List.of(0), PAGE_SIZE, 15000, 0.15, 8 * PAGE_SIZE),
                false, NoOpDiskCacheMonitoringService.INSTANCE);
        lsmBtree.create();
        lsmBtree.activate();
        accessor = lsmBtree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
    }

    @After
    public void tearDown() throws Exception {
        try {
            accessor.destroy();
            lsmBtree.deactivate();
            lsmBtree.destroy();
        } finally {
            columnBufferPool.close();
            diskBufferCache.close();
            harness.tearDown();
        }
    }

    /** A field first written after a range cursor's first search must read back in its second. */
    @Test
    public void aFieldWrittenBetweenRangeSearchesReadsBack() throws Exception {
        try (Scan scan = new Scan(ID_AND_B)) {
            assertEquals(100, writeTwoComponentsAndRun(scan, null).size());
            replaceBothComponentsWithOneHoldingB();
            List<String> fresh;
            try (Scan freshScan = new Scan(ID_AND_B)) {
                fresh = freshScan.run(null);
            }
            assertTrue("a fresh cursor reads b: " + fresh.get(149), fresh.get(149).contains("\"b\""));
            assertEquals(fresh, scan.run(null));
        }
    }

    /** The same through point lookups, as an index lookup searches the primary index once per key. */
    @Test
    public void aFieldWrittenBetweenPointSearchesReadsBack() throws Exception {
        try (Scan lookup = new Scan(ID_AND_B)) {
            assertEquals(1, writeTwoComponentsAndRun(lookup, 7L).size());
            replaceBothComponentsWithOneHoldingB();
            List<String> found = lookup.run(149L);
            assertEquals(1, found.size());
            assertTrue("a reused point cursor reads b: " + found.get(0), found.get(0).contains("\"b\""));
        }
    }

    /** The same through a batched lookup, as an index lookup searches the primary index once per frame of keys. */
    @Test
    public void aFieldWrittenBetweenBatchPointSearchesReadsBack() throws Exception {
        try (Scan lookup = new Scan(ID_AND_B, true)) {
            assertEquals(1, writeTwoComponentsAndRun(lookup, 7L).size());
            replaceBothComponentsWithOneHoldingB();
            List<String> found = lookup.run(149L);
            assertEquals(1, found.size());
            assertTrue("a reused batch point cursor reads b: " + found.get(0), found.get(0).contains("\"b\""));
        }
    }

    /**
     * A range cursor kept across searches lists one tuple per disk component it now reads, as a merge reads them: not
     * also those of the components its rebuilt slots held before.
     */
    @Test
    public void aReusedRangeCursorListsTheTuplesOfItsCurrentComponents() throws Exception {
        try (Scan scan = new Scan(ID_AND_B)) {
            LSMColumnBTreeRangeSearchCursor cursor =
                    new LSMColumnBTreeRangeSearchCursor(scan.searchAccessor.getOpContext());
            try {
                insert(0, 50, false);
                accessor.scheduleFlush();
                insert(50, 50, false);
                accessor.scheduleFlush();
                assertEquals(100, scanAll(scan, cursor));
                assertEquals(2, cursor.getComponentTupleList().size());
                replaceBothComponentsWithOneHoldingB();
                assertEquals(150, scanAll(scan, cursor));
                assertEquals(2, cursor.getComponentTupleList().size());
            } finally {
                cursor.destroy();
            }
        }
    }

    private static int scanAll(Scan scan, IIndexCursor cursor) throws HyracksDataException {
        int records = 0;
        scan.searchAccessor.search(cursor, new RangePredicate(null, null, true, true, null, null));
        try {
            while (cursor.hasNext()) {
                cursor.next();
                records++;
            }
        } finally {
            cursor.close();
        }
        return records;
    }

    private List<String> writeTwoComponentsAndRun(Scan scan, Long key) throws Exception {
        insert(0, 50, false);
        accessor.scheduleFlush();
        insert(50, 50, false);
        accessor.scheduleFlush();
        return scan.run(key);
    }

    /** One merge and one flush: two disk components again, neither of them the ones the cursor first read. */
    private void replaceBothComponentsWithOneHoldingB() throws Exception {
        accessor.scheduleFullMerge();
        insert(100, 50, true);
        accessor.scheduleFlush();
        assertEquals("the component count must hold, or the cursor rebuilds its accessors anyway", 2,
                lsmBtree.getDiskComponents().size());
    }

    private void insert(int from, int count, boolean withB) throws Exception {
        StringBuilder adm = new StringBuilder();
        for (int d = from; d < from + count; d++) {
            adm.append("{\"id\": ").append(d).append(", \"a\": ").append(d);
            adm.append(withB ? ", \"b\": " + d : "").append("}\n");
        }
        IStreamDataParser parser = new ADMDataParser(KEYED_TYPE, true);
        parser.setInputStream(new ByteArrayInputStream(adm.toString().getBytes(StandardCharsets.UTF_8)));
        ArrayBackedValueStorage record = new ArrayBackedValueStorage();
        ArrayTupleBuilder tupleBuilder = new ArrayTupleBuilder(2);
        ArrayTupleReference tuple = new ArrayTupleReference();
        for (int d = from; parser.parse(record.getDataOutput()); d++) {
            tupleBuilder.reset();
            tupleBuilder.getDataOutput().writeByte(ATypeTag.SERIALIZED_INT64_TYPE_TAG);
            tupleBuilder.getDataOutput().writeLong(d);
            tupleBuilder.addFieldEndOffset();
            tupleBuilder.addField(record.getByteArray(), record.getStartOffset(), record.getLength());
            tuple.reset(tupleBuilder.getFieldEndOffsets(), tupleBuilder.getByteArray());
            accessor.upsert(tuple);
            record.reset();
        }
    }

    /** A scan whose accessor and cursor are kept across {@link #run}s, as an operator keeps them. */
    private final class Scan implements AutoCloseable {
        private final QueryColumnTupleProjector projector;
        private final ILSMIndexAccessor searchAccessor;
        private final IIndexCursor cursor;

        private final boolean batch;

        Scan(ARecordType requested) throws HyracksDataException {
            this(requested, false);
        }

        /** {@code batch}: search as the batched point lookup operator does, with its cursor and a frame of keys. */
        Scan(ARecordType requested, boolean batch) throws HyracksDataException {
            this.batch = batch;
            IndexAccessParameters iap =
                    new IndexAccessParameters(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
            projector = new QueryColumnTupleProjector(KEYED_TYPE, 1, requested, Collections.emptyMap(),
                    NoOpColumnFilterEvaluatorFactory.INSTANCE, NoOpColumnFilterEvaluatorFactory.INSTANCE,
                    NoOpWarningCollector.INSTANCE, null, ColumnProjectorType.QUERY);
            iap.getParameters().put(HyracksConstants.TUPLE_PROJECTOR, projector);
            searchAccessor = lsmBtree.createAccessor(iap);
            cursor = batch ? lsmBtree.createBatchPointSearchCursor(searchAccessor.getOpContext())
                    : searchAccessor.createSearchCursor(false);
        }

        /** The record keyed {@code key}, or every record in key order: its key, then the projected record as JSON. */
        List<String> run(Long key) throws IOException {
            List<String> out = new ArrayList<>();
            ArrayTupleBuilder projected = new ArrayTupleBuilder(2);
            RangePredicate predicate = new RangePredicate(null, null, true, true, null, null);
            if (key != null) {
                ArrayTupleBuilder keyBuilder = new ArrayTupleBuilder(1);
                keyBuilder.getDataOutput().writeByte(ATypeTag.SERIALIZED_INT64_TYPE_TAG);
                keyBuilder.getDataOutput().writeLong(key);
                keyBuilder.addFieldEndOffset();
                ArrayTupleReference keyTuple = new ArrayTupleReference();
                keyTuple.reset(keyBuilder.getFieldEndOffsets(), keyBuilder.getByteArray());
                MultiComparator cmp = MultiComparator.create(CMP_FACTORIES);
                predicate = new RangePredicate(keyTuple, keyTuple, true, true, cmp, cmp);
                if (batch) {
                    predicate = batchOf(keyTuple, cmp);
                }
            }
            try {
                searchAccessor.search(cursor, predicate);
                while (cursor.hasNext()) {
                    cursor.next();
                    projected.reset();
                    if (projector.project(cursor.getTuple(), projected.getDataOutput(), projected) == null) {
                        continue;
                    }
                    int[] ends = projected.getFieldEndOffsets();
                    long id = LongPointable.getLong(projected.getByteArray(), 1);
                    out.add(id + " " + json(projected.getByteArray(), ends[0], ends[1] - ends[0]));
                }
            } finally {
                cursor.close();
            }
            return out;
        }

        private BatchPredicate batchOf(ArrayTupleReference keyTuple, MultiComparator cmp) throws HyracksDataException {
            FixedSizeFrame frame = new FixedSizeFrame(ByteBuffer.allocate(PAGE_SIZE));
            FrameTupleAppender appender = new FrameTupleAppender(frame, true);
            assertTrue(appender.append(keyTuple));
            FrameTupleAccessor keys = new FrameTupleAccessor(new RecordDescriptor(new ISerializerDeserializer[1]));
            keys.reset(frame.getBuffer());
            BatchPredicate predicate = new BatchPredicate(keys, cmp, null, null, null);
            // As the operator does before each frame: it starts the predicate before the first key.
            predicate.reset(keys);
            return predicate;
        }

        @Override
        public void close() throws HyracksDataException {
            try {
                cursor.destroy();
            } finally {
                searchAccessor.destroy();
            }
        }
    }

    private static String json(byte[] bytes, int start, int length) throws HyracksDataException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (PrintStream print = new PrintStream(out, false, StandardCharsets.UTF_8)) {
            CleanJSONPrinterFactoryProvider.INSTANCE.getPrinterFactory(null).createPrinter(null).print(bytes, start,
                    length, print);
        }
        return out.toString(StandardCharsets.UTF_8);
    }
}
