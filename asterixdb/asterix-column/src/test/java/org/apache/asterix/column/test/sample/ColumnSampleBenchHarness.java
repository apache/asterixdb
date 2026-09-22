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
package org.apache.asterix.column.test.sample;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.builders.RecordBuilder;
import org.apache.asterix.column.ColumnManagerFactory;
import org.apache.asterix.column.filter.NoOpColumnFilterEvaluatorFactory;
import org.apache.asterix.column.operation.query.QueryColumnTupleProjector;
import org.apache.asterix.common.exceptions.NoOpWarningCollector;
import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.NullIntrospector;
import org.apache.asterix.formats.nontagged.TypeTraitProvider;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.io.IIOManager;
import org.apache.hyracks.api.util.CleanupUtils;
import org.apache.hyracks.api.util.HyracksConstants;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.LongPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.storage.am.btree.impls.BTreeOpContext;
import org.apache.hyracks.storage.am.btree.impls.RangePredicate;
import org.apache.hyracks.storage.am.common.api.ITreeIndexCursor;
import org.apache.hyracks.storage.am.common.impls.IndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.impls.NoOpOperationCallback;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.common.util.ResourceReleaseUtils;
import org.apache.hyracks.storage.am.lsm.btree.column.api.IColumnManagerFactory;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.ColumnProjectorType;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnProjectionInfo;
import org.apache.hyracks.storage.am.lsm.btree.column.api.projection.IColumnTupleProjector;
import org.apache.hyracks.storage.am.lsm.btree.column.cloud.buffercache.IColumnReadContext;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBTree;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.ColumnBtreeSampleCursor;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.btree.SampleCursorStats;
import org.apache.hyracks.storage.am.lsm.btree.column.impls.lsm.LSMColumnBTreeOpContext;
import org.apache.hyracks.storage.am.lsm.btree.column.utils.LSMColumnBTreeUtil;
import org.apache.hyracks.storage.am.lsm.btree.impls.AntimatterAwareTupleAcceptor;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTree;
import org.apache.hyracks.storage.am.lsm.btree.impls.LSMBTreeBatchPointSearchCursor;
import org.apache.hyracks.storage.am.lsm.btree.util.LSMBTreeTestHarness;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMDiskComponent;
import org.apache.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;
import org.apache.hyracks.storage.am.lsm.common.api.IVirtualBufferCache;
import org.apache.hyracks.storage.am.lsm.common.impls.DiskComponentMetadata;
import org.apache.hyracks.storage.am.lsm.common.impls.VirtualBufferCache;
import org.apache.hyracks.storage.common.buffercache.BufferCache;
import org.apache.hyracks.storage.common.buffercache.ClockPageReplacementStrategy;
import org.apache.hyracks.storage.common.buffercache.ColumnBufferPool;
import org.apache.hyracks.storage.common.buffercache.DefaultDiskCachedPageAllocator;
import org.apache.hyracks.storage.common.buffercache.DelayPageCleanerPolicy;
import org.apache.hyracks.storage.common.buffercache.HeapBufferAllocator;
import org.apache.hyracks.storage.common.buffercache.IBufferCache;
import org.apache.hyracks.storage.common.buffercache.IColumnBufferPool;
import org.apache.hyracks.storage.common.buffercache.IPageReplacementStrategy;
import org.apache.hyracks.storage.common.buffercache.context.read.DefaultBufferCacheReadContextProvider;
import org.apache.hyracks.storage.common.compression.NoOpCompressorDecompressorFactory;
import org.apache.hyracks.storage.common.disk.NoOpDiskCacheMonitoringService;
import org.apache.hyracks.storage.common.file.FileMapManager;
import org.apache.hyracks.storage.common.file.IFileMapManager;
import org.apache.hyracks.util.trace.ITracer;

/**
 * Stands up a real column LSM BTree whose oldest disk component is partially shadowed by newer ones, and drives
 * the real {@code ColumnBtreeSampleCursor} over it. Setup and cursor driving only — assertions belong in the
 * tests that consume this.
 * <p>
 * <b>Why not just use {@link LSMBTreeTestHarness}?</b> It is configured for 256-byte pages and a ~256 KB total
 * budget ({@code AccessMethodTestsConfig.LSM_BTREE_*}); a column mega-leaf does not fit in 256 bytes and the
 * budget cannot hold the records this bench needs. So the page-size-dependent pieces — disk and virtual buffer
 * caches — are built locally with column-sized pages, and everything else is taken from the shared harness,
 * which is never modified.
 * <p>
 * <b>Fidelity contract — no real disk I/O.</b> The disk buffer cache is sized from the tuples the index actually
 * <em>stores</em>, {@code numKeys + }{@link #getShadowBoundary()}: every shadowed key is stored twice, so at
 * {@code shadowPercent = 100} the index holds {@code 2 × numKeys}. The whole index therefore stays resident and
 * every page the sampler touches is a cache hit — which is what makes per-phase attribution stable enough to
 * compare runs, but also means wall times here are a lower bound on production and nothing of the form "phase X
 * is I/O bound" can be concluded. {@code ColumnSampleBenchHarnessResidencyTest} enforces it against the real
 * allocation counters rather than trusting the formula.
 * <p>
 * <b>Scaling:</b> each insert round must fit one in-memory component, so the virtual buffer caches are sized
 * from {@code numKeys} ({@link #memoryPagesFor(int)}); an overflow would auto-flush mid-round and change the
 * shadow structure's shape. Memory runs ~{@code 2 * numKeys * 256 B}, so 600 000 keys holds ~300 MB against the
 * 4 GB test heap.
 */
public class ColumnSampleBenchHarness {

    /** Column pages are buffer-cache pages; 32 KB matches the product default. */
    public static final int PAGE_SIZE = 32 * 1024;
    /** Floors for the buffer budgets; both scale up with {@code numKeys} (see {@link #memoryPagesFor(int)}). */
    static final int MIN_DISK_NUM_PAGES = 1024;
    static final int MIN_MEM_NUM_PAGES = 256;
    /**
     * Deliberately generous: measured well under 128 B (tagged PK + ~50-byte closed record + slot overhead),
     * doubled for margin. A round that does not fit one memory component auto-flushes mid-round and changes the
     * shadow structure's shape, so this is sized to guarantee fit, not to be tight.
     */
    private static final int MEMORY_BYTES_PER_TUPLE = 256;
    /**
     * On-disk per-<b>stored</b>-tuple budget, keeping the index buffer-cache resident. Derived from the footprint
     * {@link #getIndexPageCount()} actually measures, not estimated: 20.5-23.0 B observed across
     * {@code shadowPercent} 55/90/100 × {@code numShadowComponents} 3/6/8 at 20k/165k/650k keys (the column
     * encoding compresses the ascending int64 PK hard), rounded up to 32 B for a denser record shape, then a 3×
     * safety factor. An unmeasured configuration would have to be 4× heavier per tuple to threaten residency.
     * <p>
     * Applied to {@code numKeys + shadowBoundary}, not {@code numKeys}: shadowed keys are stored twice. A sizing
     * input only — the contract is enforced against the real allocator, not this estimate.
     */
    private static final int DISK_BYTES_PER_STORED_TUPLE = 96;
    private static final int NUM_MUTABLE_COMPONENTS = 2;
    private static final int MAX_OPEN_FILES = Integer.MAX_VALUE;
    private static final int IO_QUEUE_LEN = 10;
    private static final double BLOOM_FILTER_FALSE_POSITIVE_RATE = 0.01;

    // Column manager knobs (mirrors DatasetFormatInfo defaults closely enough for a bench).
    private static final int MAX_TUPLE_COUNT = 15000;
    private static final double TOLERANCE = 0.15;
    private static final int MAX_LEAF_NODE_SIZE = 8 * PAGE_SIZE;

    // Granule matched to PAGE_SIZE so one credit is one page. Pool size and memory doubled over the brief's
    // guess because ALL_COLUMNS loads whole column mega-pages per sampled page, genuinely exercising the pool.
    // Timeout at 2 minutes so credit pressure on a slow CI machine cannot become a spurious mid-run failure.
    private static final int COLUMN_BUFFER_GRANULE_BYTES = PAGE_SIZE;
    private static final int COLUMN_BUFFER_POOL_SIZE = 128;
    private static final long COLUMN_BUFFER_MAX_MEMORY = 128L * 1024 * 1024;
    private static final long COLUMN_BUFFER_RESERVE_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(2);

    // Sample cursor knobs, defaulted to the values production ships in StorageProperties so the baseline this
    // harness measures is the one the product runs. The cursor raises the attempt ceiling itself to
    // max(configured, 32 * max(leafPages, target)).
    // Matches StorageProperties.STORAGE_MAX_SAMPLE_LEAF_ATTEMPTS's default.
    private static final int DEFAULT_MAX_LEAF_ATTEMPTS = 500;
    // Matches StorageProperties.STORAGE_SAMPLE_LEAF_DRAW_BATCH_SIZE's default.
    private static final int DEFAULT_LEAF_DRAW_BATCH_SIZE = 32768;

    private static final String PK_FIELD = "id";
    private static final String PAYLOAD_FIELD = "payload";
    private static final String PAYLOAD_PREFIX = "payload-value-";

    private final int numKeys;
    private final int shadowPercent;
    private final int numShadowComponents;

    private final LSMBTreeTestHarness harness = new LSMBTreeTestHarness();

    // Tuple construction scratch state (single-threaded by construction).
    private final RecordBuilder recordBuilder = new RecordBuilder();
    private final UTF8StringSerializerDeserializer stringSerde = new UTF8StringSerializerDeserializer();
    private final ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
    private final ArrayTupleBuilder insertTupleBuilder = new ArrayTupleBuilder(2);
    private final ArrayTupleReference insertTuple = new ArrayTupleReference();
    private final ArrayTupleBuilder deleteTupleBuilder = new ArrayTupleBuilder(1);
    private final ArrayTupleReference deleteTuple = new ArrayTupleReference();

    private final Projection projection;

    private int maxLeafAttempts = DEFAULT_MAX_LEAF_ATTEMPTS;
    private int leafDrawBatchSize = DEFAULT_LEAF_DRAW_BATCH_SIZE;
    /** {@code < 0} means "read the real value from the sampled component's metadata", as production does. */
    private int maxLeafTupleCountOverride = -1;

    private ARecordType recordType;
    private IBufferCache diskBufferCache;
    private IPageReplacementStrategy pageReplacementStrategy;
    private IColumnBufferPool columnBufferPool;
    private List<IVirtualBufferCache> virtualBufferCaches;
    private LSMBTree lsmBtree;

    /**
     * What the sample scan projects out of each sampled row, i.e. how much column data phase 2 must materialize.
     */
    public enum Projection {
        /**
         * Every column, via the same projector production's sample collector installs: a
         * {@code QueryColumnTupleProjector} over {@link ProjectionFiltrationTypeUtil#ALL_FIELDS_TYPE} reporting
         * {@link ColumnProjectorType#MODIFY}. That is what
         * {@code BTreeSampleCollectorOperatorDescriptorNodePushable} ends up with:
         * {@code BTreeSearchOperatorNodePushable#addAdditionalIndexAccessorParams} puts the descriptor's tuple
         * projector under {@code HyracksConstants.TUPLE_PROJECTOR}, and for a columnar dataset
         * {@code IndexUtil#createPrimaryIndexScanTupleProjectorFactory} makes that a
         * {@code PrimaryScanColumnTupleProjectorFactory}, so {@code ColumnUtil#getTupleProjector} finds a
         * projector and never falls back to {@code columnManager.getMergeColumnProjector()}. The sample index
         * stores whole records, hence all fields. This is the faithful baseline.
         */
        ALL_COLUMNS,
        /**
         * Primary key only. Phase 2 then loads no column mega-pages, which isolates phase 1 (leaf draws + liveness
         * + PK seeks) but makes {@code livenessSharePct()} an upper bound rather than a real share.
         */
        PK_ONLY
    }

    /** Equivalent to {@code ColumnSampleBenchHarness(numKeys, shadowPercent, numShadowComponents, ALL_COLUMNS)}. */
    public ColumnSampleBenchHarness(int numKeys, int shadowPercent, int numShadowComponents) {
        this(numKeys, shadowPercent, numShadowComponents, Projection.ALL_COLUMNS);
    }

    public ColumnSampleBenchHarness(int numKeys, int shadowPercent, int numShadowComponents, Projection projection) {
        this.numKeys = numKeys;
        this.shadowPercent = shadowPercent;
        this.numShadowComponents = numShadowComponents;
        this.projection = projection;
    }

    public void setUp() throws Exception {
        // Note: the shared harness's own 256-byte-page disk buffer cache is created here and left unused (it is
        // closed again by harness.tearDown()); only its page-size-independent pieces are consumed below.
        harness.setUp();
        IIOManager ioManager = harness.getIOManager();
        diskBufferCache = createDiskBufferCache(ioManager, diskPagesFor((long) numKeys + getShadowBoundary()));
        columnBufferPool = new ColumnBufferPool(COLUMN_BUFFER_GRANULE_BYTES, COLUMN_BUFFER_POOL_SIZE,
                COLUMN_BUFFER_MAX_MEMORY, COLUMN_BUFFER_RESERVE_TIMEOUT_MILLIS);
        virtualBufferCaches = new ArrayList<>();
        int memPages = memoryPagesFor(numKeys);
        for (int i = 0; i < NUM_MUTABLE_COMPONENTS; i++) {
            virtualBufferCaches.add(new VirtualBufferCache(new HeapBufferAllocator(), PAGE_SIZE, memPages));
        }

        recordType = new ARecordType("bench", new String[] { PK_FIELD, PAYLOAD_FIELD },
                new IAType[] { BuiltinType.AINT64, BuiltinType.ASTRING }, false);
        ITypeTraits[] typeTraits = new ITypeTraits[] { TypeTraitProvider.INSTANCE.getTypeTrait(BuiltinType.AINT64),
                TypeTraitProvider.INSTANCE.getTypeTrait(recordType) };
        IBinaryComparatorFactory[] cmpFactories = new IBinaryComparatorFactory[] {
                BinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(BuiltinType.AINT64, true) };
        int[] bloomFilterKeyFields = new int[] { 0 };

        IColumnManagerFactory columnManagerFactory = new ColumnManagerFactory(recordType, null,
                Collections.singletonList(Collections.singletonList(PK_FIELD)), Collections.singletonList(0), PAGE_SIZE,
                MAX_TUPLE_COUNT, TOLERANCE, MAX_LEAF_NODE_SIZE);

        lsmBtree = LSMColumnBTreeUtil.createLSMTree(harness.getNCConfig(), ioManager, virtualBufferCaches,
                harness.getFileReference(), diskBufferCache, columnBufferPool, typeTraits, cmpFactories,
                bloomFilterKeyFields, BLOOM_FILTER_FALSE_POSITIVE_RATE, harness.getMergePolicy(),
                harness.getOperationTracker(), harness.getIOScheduler(), harness.getIOOperationCallbackFactory(),
                harness.getPageWriteCallbackFactory(), null /* btreeFields */, harness.getMetadataPageManagerFactory(),
                false /* updateAware */, ITracer.NONE, NoOpCompressorDecompressorFactory.INSTANCE,
                TypeTraitProvider.INSTANCE.getTypeTrait(BuiltinType.ANULL), NullIntrospector.INSTANCE,
                columnManagerFactory, false /* atomic */, NoOpDiskCacheMonitoringService.INSTANCE);
        lsmBtree.create();
        lsmBtree.activate();
        buildComponents();
    }

    /**
     * Builds the base disk component (all {@code numKeys} keys) followed by {@code numShadowComponents} newer disk
     * components that delete-and-reinsert the first {@code numKeys * shadowPercent / 100} keys, so those keys are
     * shadowed in the newer components and must not be emitted when the oldest component is sampled.
     */
    private void buildComponents() throws HyracksDataException {
        ILSMIndexAccessor accessor = lsmBtree.createAccessor(NoOpIndexAccessParameters.INSTANCE);
        for (int key = 0; key < numKeys; key++) {
            insert(accessor, key);
        }
        accessor.scheduleFlush();

        int shadowBoundary = getShadowBoundary();
        int perComponent = numShadowComponents == 0 ? 0 : (int) divideRoundingUp(shadowBoundary, numShadowComponents);
        for (int component = 0; component < numShadowComponents; component++) {
            int from = component * perComponent;
            int to = Math.min(shadowBoundary, from + perComponent);
            for (int key = from; key < to; key++) {
                delete(accessor, key);
                insert(accessor, key);
            }
            accessor.scheduleFlush();
        }
    }

    /** Keys in {@code [0, shadowBoundary)} are shadowed by a newer component; keys at or above it stay live. */
    public int getShadowBoundary() {
        return numKeys * shadowPercent / 100;
    }

    private static long divideRoundingUp(long dividend, long divisor) {
        return (dividend + divisor - 1) / divisor;
    }

    private void insert(ILSMIndexAccessor accessor, int key) throws HyracksDataException {
        insertTupleBuilder.reset();
        writeTaggedInt64(insertTupleBuilder.getDataOutput(), key);
        insertTupleBuilder.addFieldEndOffset();
        writeRecord(insertTupleBuilder.getDataOutput(), key);
        insertTupleBuilder.addFieldEndOffset();
        insertTuple.reset(insertTupleBuilder.getFieldEndOffsets(), insertTupleBuilder.getByteArray());
        accessor.insert(insertTuple);
    }

    private void delete(ILSMIndexAccessor accessor, int key) throws HyracksDataException {
        deleteTupleBuilder.reset();
        writeTaggedInt64(deleteTupleBuilder.getDataOutput(), key);
        deleteTupleBuilder.addFieldEndOffset();
        deleteTuple.reset(deleteTupleBuilder.getFieldEndOffsets(), deleteTupleBuilder.getByteArray());
        accessor.delete(deleteTuple);
    }

    private void writeRecord(DataOutput out, int key) throws HyracksDataException {
        recordBuilder.reset(recordType);
        recordBuilder.init();
        fieldValue.reset();
        writeTaggedInt64(fieldValue.getDataOutput(), key);
        recordBuilder.addField(0, fieldValue);
        fieldValue.reset();
        writeTaggedString(fieldValue.getDataOutput(), PAYLOAD_PREFIX + key);
        recordBuilder.addField(1, fieldValue);
        recordBuilder.write(out, true);
    }

    private static void writeTaggedInt64(DataOutput out, long value) throws HyracksDataException {
        try {
            out.writeByte(ATypeTag.SERIALIZED_INT64_TYPE_TAG);
            out.writeLong(value);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    private void writeTaggedString(DataOutput out, String value) throws HyracksDataException {
        try {
            out.writeByte(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
            stringSerde.serialize(value, out);
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
    }

    public void tearDown() throws Exception {
        try {
            if (lsmBtree != null) {
                lsmBtree.deactivate();
                lsmBtree.destroy();
            }
        } finally {
            try {
                if (columnBufferPool != null) {
                    columnBufferPool.close();
                }
                if (diskBufferCache != null) {
                    diskBufferCache.close();
                }
            } finally {
                harness.tearDown();
            }
        }
    }

    /** Result of one sample drive over the oldest disk component. */
    public static final class SampleRun {
        public final int emitted;
        public final int[] sampledKeys;
        public final long wallNanos;

        SampleRun(int emitted, int[] sampledKeys, long wallNanos) {
            this.emitted = emitted;
            this.sampledKeys = sampledKeys;
            this.wallNanos = wallNanos;
        }
    }

    /**
     * Drives the real column sample cursor over the <b>oldest</b> disk component with the cross-component liveness
     * search pointed at <b>all</b> newer components — the post-mutation shape whose cost is under study. Mirrors
     * {@link org.apache.hyracks.storage.am.lsm.btree.impls.LSMIndexSampleCursor}'s wiring for the last component.
     *
     * @param targetCardinality number of live tuples the sampler should try to collect
     * @param seed              sample seed
     * @param stats             cost-breakdown holder attached to the cursor; pass a fresh instance per run
     */
    public SampleRun runSample(int targetCardinality, long seed, SampleCursorStats stats) throws HyracksDataException {
        LSMColumnBTreeOpContext opCtx = createSearchOpContext();
        List<ILSMComponent> components = opCtx.getComponentHolder(); // newest first
        int oldestIndex = components.size() - 1;
        ILSMComponent oldest = components.get(oldestIndex);
        List<ILSMComponent> newer = new ArrayList<>(components.subList(0, oldestIndex));
        RangePredicate searchPred = new RangePredicate(null, null, true, true, null, null);

        // The cursor cannot emit more than the target, so the result array is exactly sized.
        int[] sampledKeys = new int[Math.max(1, targetCardinality)];
        int emitted = 0;
        long startNanos = 0;
        long wallNanos = 0;
        // Everything acquired below is released in the finally; acquisition happens INSIDE the try so a failure
        // part-way through cannot leak a cursor holding pinned pages (which would in turn make the buffer-cache
        // close in tearDown() complain about unpinned pages and bury the original failure).
        LSMBTreeBatchPointSearchCursor searchCursor = null;
        ColumnBTree.ColumnBTreeAccessor sampleAccessor = null;
        ITreeIndexCursor sampleCursor = null;
        try {
            opCtx.getSearchInitialState().reset(searchPred, newer);
            // The same factory LSMIndexSampleCursor uses, so the harness measures the liveness cursor
            // production actually gets (existence-only, key-only projection) rather than the query cursor.
            searchCursor = lsmBtree.createSampleLivenessSearchCursor(opCtx);
            searchCursor.open(opCtx.getSearchInitialState(), searchPred);

            ColumnBTree columnBTree = (ColumnBTree) oldest.getIndex();
            sampleAccessor = (ColumnBTree.ColumnBTreeAccessor) columnBTree
                    .createAccessor(NoOpIndexAccessParameters.INSTANCE, opCtx, oldestIndex);
            sampleCursor = sampleAccessor.createSampleCursor(targetCardinality, seed, searchCursor, maxLeafAttempts,
                    leafDrawBatchSize, resolveMaxLeafTupleCount(oldest), AntimatterAwareTupleAcceptor.INSTANCE);
            ((ColumnBtreeSampleCursor) sampleCursor).setStats(stats);

            startNanos = System.nanoTime();
            sampleAccessor.diskSampleScan(sampleCursor);
            while (sampleCursor.hasNext()) {
                sampleCursor.next();
                ITupleReference tuple = sampleCursor.getTuple();
                // Field 0 is the tagged primary key: one type-tag byte followed by the 8-byte value.
                long key = LongPointable.getLong(tuple.getFieldData(0), tuple.getFieldStart(0) + 1);
                sampledKeys[emitted++] = (int) key;
            }
            // Recorded before teardown so the reported wall time covers the scan only.
            wallNanos = System.nanoTime() - startNanos;
        } finally {
            Throwable failure = null;
            failure = ResourceReleaseUtils.close(sampleCursor, failure);
            failure = CleanupUtils.destroy(failure, sampleCursor, sampleAccessor);
            failure = ResourceReleaseUtils.close(searchCursor, failure);
            failure = CleanupUtils.destroy(failure, searchCursor);
            if (failure != null) {
                throw HyracksDataException.create(failure);
            }
        }
        return new SampleRun(emitted, Arrays.copyOf(sampledKeys, emitted), wallNanos);
    }

    /**
     * Number of mega-leaf pages in the oldest (sampled) disk component. The sampler draws uniformly over these, so
     * this is the population size a leaf-page uniformity check must be computed against.
     */
    public int getSampledComponentLeafPageCount() throws HyracksDataException {
        return getSampledComponentLeafPageIds().length;
    }

    /**
     * The mega-leaf page ids of the oldest (sampled) disk component, in key order — the same enumeration the
     * sample cursor draws from. Exposed so a test can position a read leaf frame on a real mega-leaf page and
     * observe what a given {@link IColumnReadContext} pins for it.
     */
    public int[] getSampledComponentLeafPageIds() throws HyracksDataException {
        LSMColumnBTreeOpContext opCtx = createSearchOpContext();
        List<ILSMComponent> components = opCtx.getComponentHolder();
        int oldestIndex = components.size() - 1;
        ColumnBTree columnBTree = (ColumnBTree) components.get(oldestIndex).getIndex();
        IColumnProjectionInfo projectionInfo = opCtx.createProjectionInfo();
        IColumnReadContext readContext = opCtx.createPageZeroContext(projectionInfo);
        ColumnBTree.ColumnBTreeAccessor accessor = (ColumnBTree.ColumnBTreeAccessor) columnBTree
                .createAccessor(NoOpIndexAccessParameters.INSTANCE, oldestIndex, projectionInfo, readContext);
        try {
            BTreeOpContext btreeOpCtx = accessor.getOpContext();
            btreeOpCtx.reset();
            return columnBTree.enumerateLeafPageIds(columnBTree.getRootPageId(), btreeOpCtx, readContext);
        } finally {
            // Currently safe either way (leaf-page enumeration unpins symmetrically and no columns are
            // prepared), but release explicitly so this stays safe if the method is later extended to do more
            // than enumerate leaf page ids.
            Throwable failure = CleanupUtils.destroy(null, accessor);
            try {
                readContext.close(diskBufferCache);
            } catch (Throwable t) {
                failure = failure == null ? t : failure;
            }
            if (failure != null) {
                throw HyracksDataException.create(failure);
            }
        }
    }

    /**
     * Search op context carrying the configured tuple projector, with the operational components resolved
     * (newest first). Public so tests can inspect what the sampling wiring derives from it (the projection info
     * and the cursors built off it) without duplicating the setup.
     */
    public LSMColumnBTreeOpContext createSearchOpContext() throws HyracksDataException {
        IndexAccessParameters iap =
                new IndexAccessParameters(NoOpOperationCallback.INSTANCE, NoOpOperationCallback.INSTANCE);
        // Both modes install a projector, because production's sample collector does: see Projection.ALL_COLUMNS.
        // Installing nothing would exercise columnManager.getMergeColumnProjector(), which on this path only the
        // row-format collector (DefaultTupleProjectorFactory, not an IColumnTupleProjector) ever reaches.
        iap.getParameters().put(HyracksConstants.TUPLE_PROJECTOR,
                projection == Projection.PK_ONLY ? createPrimaryKeyOnlyProjector() : createAllColumnsProjector());
        LSMColumnBTreeOpContext opCtx = (LSMColumnBTreeOpContext) lsmBtree.createOpContext(iap);
        opCtx.setOperation(IndexOperation.SEARCH);
        lsmBtree.getOperationalComponents(opCtx);
        return opCtx;
    }

    /**
     * Mirrors {@code PrimaryScanColumnTupleProjector} — what
     * {@code IndexUtil#createPrimaryIndexScanTupleProjectorFactory} builds for a columnar dataset — which is
     * package-private, hence rebuilt here rather than reused.
     */
    private IColumnTupleProjector createAllColumnsProjector() {
        return new QueryColumnTupleProjector(recordType, 1 /* numberOfPrimaryKeys */,
                ProjectionFiltrationTypeUtil.ALL_FIELDS_TYPE, Collections.emptyMap(),
                NoOpColumnFilterEvaluatorFactory.INSTANCE, NoOpColumnFilterEvaluatorFactory.INSTANCE,
                NoOpWarningCollector.INSTANCE, null /* no task context: no filters are evaluated */,
                ColumnProjectorType.MODIFY);
    }

    private IColumnTupleProjector createPrimaryKeyOnlyProjector() {
        return new QueryColumnTupleProjector(recordType, 1 /* numberOfPrimaryKeys */,
                ProjectionFiltrationTypeUtil.EMPTY_TYPE, Collections.emptyMap(),
                NoOpColumnFilterEvaluatorFactory.INSTANCE, NoOpColumnFilterEvaluatorFactory.INSTANCE,
                NoOpWarningCollector.INSTANCE, null /* no task context: no filters are evaluated */,
                ColumnProjectorType.QUERY);
    }

    /**
     * Per-component max leaf tuple count, read from the sampled component's metadata exactly as
     * {@link org.apache.hyracks.storage.am.lsm.btree.impls.LSMIndexSampleCursor} does. The cursor uses it as the
     * denominator of the fill-proportional (Olken) page-acceptance probability, so passing a real value is what
     * makes {@code stats.pagesRejected} meaningful and matches the estimator production ships.
     *
     * @return the override set via {@link #setMaxLeafTupleCount(int)} if any, else the component's recorded value,
     *         else 0 (fill-rejection disabled) when the component carries no such metadata
     */
    private int resolveMaxLeafTupleCount(ILSMComponent component) throws HyracksDataException {
        if (maxLeafTupleCountOverride >= 0) {
            return maxLeafTupleCountOverride;
        }
        DiskComponentMetadata metadata = (DiskComponentMetadata) component.getMetadata();
        ArrayBackedValueStorage maxTupleCountRef = new ArrayBackedValueStorage();
        if (metadata.get(DiskComponentMetadata.MAX_LEAF_TUPLE_COUNT_KEY, maxTupleCountRef)
                && maxTupleCountRef.getLength() >= Integer.BYTES) {
            return IntegerPointable.getInteger(maxTupleCountRef.getByteArray(), maxTupleCountRef.getStartOffset());
        }
        return 0;
    }

    public LSMBTree getIndex() {
        return lsmBtree;
    }

    /**
     * The column-sized disk buffer cache the components live in. Exposed so a test can pin real mega-leaf pages
     * directly instead of going through a cursor.
     */
    public IBufferCache getDiskBufferCache() {
        return diskBufferCache;
    }

    public int getNumKeys() {
        return numKeys;
    }

    public int getShadowPercent() {
        return shadowPercent;
    }

    public int getNumShadowComponents() {
        return numShadowComponents;
    }

    public int getLeafDrawBatchSize() {
        return leafDrawBatchSize;
    }

    /** Overrides the sampler's leaf-draw batch size for the next {@link #runSample} (batch-size sweeps). */
    public void setLeafDrawBatchSize(int leafDrawBatchSize) {
        this.leafDrawBatchSize = leafDrawBatchSize;
    }

    public int getMaxLeafAttempts() {
        return maxLeafAttempts;
    }

    public void setMaxLeafAttempts(int maxLeafAttempts) {
        this.maxLeafAttempts = maxLeafAttempts;
    }

    public Projection getProjection() {
        return projection;
    }

    /**
     * Forces the sampler's {@code maxLeafTupleCount}, overriding the value read from the sampled component's
     * metadata. Pass {@code 0} to disable fill-proportional page rejection entirely, or a negative value to go
     * back to the production behaviour of reading it from the component.
     */
    public void setMaxLeafTupleCount(int maxLeafTupleCount) {
        this.maxLeafTupleCountOverride = maxLeafTupleCount;
    }

    /** The {@code maxLeafTupleCount} the next {@link #runSample} will pass to the cursor. */
    public int getEffectiveMaxLeafTupleCount() throws HyracksDataException {
        List<ILSMComponent> components = createSearchOpContext().getComponentHolder();
        return resolveMaxLeafTupleCount(components.get(components.size() - 1));
    }

    /**
     * Virtual-buffer-cache pages per mutable component, sized so one whole insert round fits in one memory
     * component (otherwise the LSM auto-flushes mid-round: the column flush path then reaches
     * {@code ColumnBTree.createAccessor(IIndexAccessParameters)} and fails outright).
     */
    static int memoryPagesFor(int numKeys) {
        return (int) Math.max(MIN_MEM_NUM_PAGES, divideRoundingUp(numKeys * MEMORY_BYTES_PER_TUPLE, PAGE_SIZE));
    }

    /**
     * Disk-buffer-cache pages, sized to keep every component resident (see the fidelity contract above).
     *
     * @param storedTuples total tuples across all components, i.e. {@code numKeys + shadowBoundary}
     */
    static int diskPagesFor(long storedTuples) {
        return (int) Math.max(MIN_DISK_NUM_PAGES,
                divideRoundingUp(storedTuples * DISK_BYTES_PER_STORED_TUPLE, PAGE_SIZE));
    }

    private IBufferCache createDiskBufferCache(IIOManager ioManager, int diskNumPages) {
        pageReplacementStrategy = new ClockPageReplacementStrategy(new HeapBufferAllocator(),
                DefaultDiskCachedPageAllocator.INSTANCE, PAGE_SIZE, diskNumPages);
        IFileMapManager fileMapManager = new FileMapManager();
        ThreadFactory threadFactory = Thread::new;
        return new BufferCache(ioManager, pageReplacementStrategy, new DelayPageCleanerPolicy(1000), fileMapManager,
                MAX_OPEN_FILES, IO_QUEUE_LEN, threadFactory, new HashMap<>(),
                DefaultBufferCacheReadContextProvider.DEFAULT);
    }

    /* *********************************************************************************
     * Buffer-cache residency instrumentation — enforces the no-real-disk-I/O contract
     * *********************************************************************************
     */

    /**
     * Number of buffer-cache pages the page-replacement strategy has <b>actually allocated</b> so far. This is
     * runtime allocator state, read straight off {@link ClockPageReplacementStrategy}, not a restatement of the
     * sizing formula: the strategy allocates lazily on demand and only ever starts evicting once this reaches
     * {@link #getBufferCachePageBudget()} (see {@code ClockPageReplacementStrategy#findVictim}, which loops while
     * {@code numPages + multiplier > maxAllowedNumPages}). So
     * {@code getAllocatedBufferCachePages() < getBufferCachePageBudget()} after a workload is direct evidence that
     * <b>no page was ever evicted</b> during it, and therefore that no measurement included a re-read from disk.
     */
    public int getAllocatedBufferCachePages() {
        return pageReplacementStrategy.getNumPages();
    }

    /** The disk buffer cache's real configured page budget. */
    public int getBufferCachePageBudget() {
        return pageReplacementStrategy.getMaxAllowedNumPages();
    }

    /**
     * Total page count of every disk component's file, read from the real file handles via
     * {@link IBufferCache#getNumPagesOfFile(int)} — the index's measured on-disk footprint, independent of the
     * {@code *_BYTES_PER_TUPLE} estimates used to size the caches.
     */
    public int getIndexPageCount() throws HyracksDataException {
        int pages = 0;
        for (ILSMDiskComponent component : lsmBtree.getDiskComponents()) {
            pages += diskBufferCache.getNumPagesOfFile(((ColumnBTree) component.getIndex()).getFileId());
        }
        return pages;
    }
}
